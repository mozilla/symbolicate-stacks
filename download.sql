declare latest_release_version NUMERIC default (select major_version from telemetry.releases where category = 'major' order by date desc limit 1);
declare min_esr_version NUMERIC default (select major_version from telemetry.releases where category = 'esr' order by date desc limit 1);
declare min_version NUMERIC default latest_release_version - 3;

begin transaction;

-- Expire old entries (in case jobs fail before removing them).
delete from moz-fx-data-shared-prod.crash_ping_ingest_external.live_ingest_log
where time < CURRENT_TIMESTAMP() - INTERVAL 2 HOUR;

create temp table selected as (
    -- TODO use crash event ID to deduplicate
    with
    all_pings as (
        select distinct
            document_id,
            -- Explicitly format the timestamp for maximum precision because these values will be round-tripped into the output table and joined.
            -- Otherwise the default result string only has millisecond precision and doesn't join correctly.
            FORMAT_TIMESTAMP("%FT%R:%E*S", submission_timestamp) as submission_timestamp,
            NULLIF(TO_JSON_STRING(metrics.object.crash_stack_traces), 'null') as stack_traces,
            NULLIF(TO_JSON_STRING(metrics.object.crash_java_exception), 'null') as java_exception,
            NULLIF(TO_JSON_STRING(metrics.object.crash_async_shutdown_timeout), 'null') as async_shutdown_timeout,
            metrics.string.crash_moz_crash_reason as moz_crash_reason,
            metrics.string.crash_ipc_channel_error as ipc_channel_error,
            metrics.quantity.memory_oom_allocation_size as oom_size,
            metrics.string.crash_hang as hang,
            coalesce(metrics.string.crash_os, normalized_os) as os,
            coalesce(metrics.string.crash_app_channel, client_info.app_channel) as channel,
            SAFE_CAST(REGEXP_SUBSTR(coalesce(metrics.string.crash_app_display_version, client_info.app_display_version), '[0-9]*') as INT64) as major_version
        from (
            select * from moz-fx-data-shared-prod.firefox_crashreporter_live.crash_v1
            union
            select * from moz-fx-data-shared-prod.fenix_live.crash_v1
            union
            select * from moz-fx-data-shared-prod.focus_android_live.crash_v1
            union
            select * from moz-fx-data-shared-prod.klar_android_live.crash_v1
        )
        where 
            submission_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
    )
    , pings as (
        select * from all_pings
        where
            ((channel = 'release' or channel = 'beta' or channel = 'nightly') and major_version >= min_version)
            or
            (channel = 'esr' and major_version >= min_esr_version)
    )

    select *
    from pings
    where document_id not in (
        select document_id from moz-fx-data-shared-prod.crash_ping_ingest_external.live_ingest_output
        union
        select document_id from moz-fx-data-shared-prod.crash_ping_ingest_external.live_ingest_log
    )
    limit 1000
);

insert into moz-fx-data-shared-prod.crash_ping_ingest_external.live_ingest_log
select document_id, current_timestamp() as time from selected;

select * from selected;

commit transaction;
