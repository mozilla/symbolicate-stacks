//! Tests using the public library interface.

use lib::{SymbolicateStacks, config::*};
use serde_json::{Value, json};

async fn test_symbolicate(input: String, expected_output: Value) {
    dbg!(&input);
    let symbolicator = SymbolicateStacks::new(Config::default()).unwrap();
    let output = symbolicator
        .symbolicate(input)
        .await
        .expect("failed to symbolicate");
    // Parse the output to do a JSON comparison (mainly so object key order doesn't matter)
    let output: Value = output.parse().expect("invalid JSON output");
    assert_eq!(output, expected_output);
}

/// Define a test which checks how an input annotations is represented in the output.
///
/// This accepts zero or more of the following form:
/// ```ignore
/// test_name() { /*input JSON*/ => /*output JSON*/ }
/// ```
///
/// The JSON values are parsed by `serde_json::json!`.
macro_rules! annotation_tests {
    () => {};
    ( $name:ident () { $invalue:tt => $outvalue:tt } $($rest:tt)* ) => {
        #[tokio::test]
        async fn $name() {
            let input = json!($invalue).to_string();
            let expected_output = json!($outvalue);
            test_symbolicate(input, expected_output).await;
        }

        annotation_tests! { $($rest)* }
    };
}

annotation_tests! {
    test_ipc_channel_error() {
        {"ipc_channel_error":"foobar"} => {"ipc_channel_error":"foobar"}
    }

    test_oom_allocation_size() {
        {"OOMAllocationSize":42} => {"oom_allocation_size":42}
    }

    test_oom_allocation_size_string() {
        {"OOMAllocationSize":"1234"} => {"oom_allocation_size":1234}
    }

    test_os() {
        {"OS":"Windows"} => {"os":"Windows"}
    }

    test_async_shutdown_timeout() {
        {"AsyncShutdownTimeout":r#"{"conditions":[{"name":"foobar"}],"phase":"profile-before-change"}"#}
        =>
        {"async_shutdown_timeout":r#"{"conditions":[{"name":"foobar"}],"phase":"profile-before-change"}"#}
    }

    // The `conditions` field may be a stringified array, but should come out un-stringified.
    test_async_shutdown_timeout_stringified_conditions() {
        {"AsyncShutdownTimeout": r#"{"conditions":"[{\"name\":\"foobar\"}]","phase":"profile-before-change"}"#}
        =>
        {"async_shutdown_timeout": r#"{"conditions":[{"name":"foobar"}],"phase":"profile-before-change"}"#}
    }

    test_hang() {
        {"Hang":"ui"} => {"hang":"ui"}
    }

    test_js_large_allocation_failure() {
        {"JSLargeAllocationFailure":"Recovered"} => {"js_large_allocation_failure":"Recovered"}
    }

    test_abort_message() {
        {"AbortMessage":"foobar"} => {"abort_message":"foobar"}
    }

    test_ipc_message_name() {
        {"IPCMessageName":"foobar"} => {"ipc_message_name":"foobar"}
    }

    test_additional_minidumps() {
        {"additional_minidumps":"browser"} => {"additional_minidumps":"browser"}
    }

    test_java_exception_v0() {
        {"JavaException": r#"{"stack":[{"className":"com.android.internal.os.RuntimeInit$MethodAndArgsCaller","file":"RuntimeInit.java","isNative":false,"line":626,"methodName":"run"},{"className":"com.android.internal.os.ZygoteInit","file":"ZygoteInit.java","isNative":false,"line":1137,"methodName":"main"}]}"#}
        =>
        {"java_exception": {"exception":{"values":[{"stacktrace":{"module":null,"type":null,"frames":[{"module":"com.android.internal.os.RuntimeInit$MethodAndArgsCaller","function":"run","filename":"RuntimeInit.java"},{"module":"com.android.internal.os.ZygoteInit","function":"main","filename":"ZygoteInit.java"}]}}]}}}
    }

    test_java_exception_v1() {
        {"JavaException": r#"{"throwables":[{"stack":[{"class_name":"mozilla.components.browser.engine.gecko.GeckoEngine$$ExternalSyntheticLambda36","file":"R8$$SyntheticClass","is_native":false,"line":5,"method_name":"onShutdown"},{"class_name":"org.mozilla.geckoview.GeckoRuntime$1","file":"GeckoRuntime.java","is_native":false,"line":31,"method_name":"handleMessage"},{"class_name":"org.mozilla.gecko.EventDispatcher$3","file":"EventDispatcher.java","is_native":false,"line":13,"method_name":"run"},{"class_name":"android.os.Handler","file":"Handler.java","is_native":false,"line":938,"method_name":"handleCallback"},{"class_name":"android.os.Handler","file":"Handler.java","is_native":false,"line":99,"method_name":"dispatchMessage"},{"class_name":"android.os.Looper","file":"Looper.java","is_native":false,"line":233,"method_name":"loop"},{"class_name":"android.app.ActivityThread","file":"ActivityThread.java","is_native":false,"line":8063,"method_name":"main"},{"class_name":"java.lang.reflect.Method","file":"Method.java","is_native":true,"line":-2,"method_name":"invoke"},{"class_name":"com.android.internal.os.RuntimeInit$MethodAndArgsCaller","file":"RuntimeInit.java","is_native":false,"line":631,"method_name":"run"},{"class_name":"com.android.internal.os.ZygoteInit","file":"ZygoteInit.java","is_native":false,"line":978,"method_name":"main"}],"type_name":"java.lang.RuntimeException"}]}"#}
        =>
        {"java_exception": {"exception":{"values":[{"stacktrace":{"module":"java.lang","type":"RuntimeException","frames":[{"module":"mozilla.components.browser.engine.gecko.GeckoEngine$$ExternalSyntheticLambda36","function":"onShutdown","filename":"R8$$SyntheticClass"},{"module":"org.mozilla.geckoview.GeckoRuntime$1","function":"handleMessage","filename":"GeckoRuntime.java"},{"module":"org.mozilla.gecko.EventDispatcher$3","function":"run","filename":"EventDispatcher.java"},{"module":"android.os.Handler","function":"handleCallback","filename":"Handler.java"},{"module":"android.os.Handler","function":"dispatchMessage","filename":"Handler.java"},{"module":"android.os.Looper","function":"loop","filename":"Looper.java"},{"module":"android.app.ActivityThread","function":"main","filename":"ActivityThread.java"},{"module":"java.lang.reflect.Method","function":"invoke","filename":"Method.java"},{"module":"com.android.internal.os.RuntimeInit$MethodAndArgsCaller","function":"run","filename":"RuntimeInit.java"},{"module":"com.android.internal.os.ZygoteInit","function":"main","filename":"ZygoteInit.java"}]}}]}}}
    }

    test_stack_traces() {
        {"StackTraces":r#"{"crash_address":"0x1260a399","crash_thread":0,"crash_type":"EXCEPTION_BREAKPOINT","main_module":0,"modules":[{"base_address":"0xea0000","code_id":"6a8ec3be137000","debug_file":"firefox.pdb","debug_id":"7BD6B2B30D0F89554C4C44205044422E1","end_address":"0xfd7000","filename":"firefox.exe","version":"155.0.0.1229"},{"base_address":"0xf8b0000","code_id":"6a8ec3ec98f4000","end_address":"0x191a4000","filename":"xul.dll","version":"155.0.0.1229"},{"base_address":"0x769b0000","code_id":"e50922789d000","debug_file":"kernel32.pdb","debug_id":"2036C2DF985F0B2A0EAED37F2E897EC41","end_address":"0x76a4d000","filename":"kernel32.dll","version":"10.0.19041.5915"},{"base_address":"0x772a0000","code_id":"5f1a1fa519f000","debug_file":"ntdll.pdb","debug_id":"EBAE1381C11394FF20DD5EFC34E32FD91","end_address":"0x7743f000","filename":"ntdll.dll","version":"10.0.19041.6456"}],"threads":[{"frames":[{"ip":"0x1260a399","module_index":1,"trust":"context"},{"ip":"0x10f86cae","module_index":1,"trust":"frame_pointer"},{"ip":"0x145679de","module_index":1,"trust":"frame_pointer"},{"ip":"0x14565241","module_index":1,"trust":"frame_pointer"},{"ip":"0x10683f5e","module_index":1,"trust":"frame_pointer"},{"ip":"0x106840b6","module_index":1,"trust":"frame_pointer"},{"ip":"0x110ac88f","module_index":1,"trust":"frame_pointer"},{"ip":"0x11a0512a","module_index":1,"trust":"frame_pointer"},{"ip":"0x114daa51","module_index":1,"trust":"frame_pointer"},{"ip":"0x110d2dcc","module_index":1,"trust":"frame_pointer"},{"ip":"0x10f455d9","module_index":1,"trust":"frame_pointer"},{"ip":"0x10f58ab3","module_index":1,"trust":"frame_pointer"},{"ip":"0xf8dd87e","module_index":1,"trust":"frame_pointer"},{"ip":"0xf8dd7ee","module_index":1,"trust":"frame_pointer"},{"ip":"0xf93dd04","module_index":1,"trust":"frame_pointer"},{"ip":"0xf93db96","module_index":1,"trust":"frame_pointer"},{"ip":"0x14b75e6c","module_index":1,"trust":"frame_pointer"},{"ip":"0x11ed2160","module_index":1,"trust":"frame_pointer"},{"ip":"0x11e74d1c","module_index":1,"trust":"frame_pointer"},{"ip":"0x11e749bb","module_index":1,"trust":"frame_pointer"},{"ip":"0x11e74940","module_index":1,"trust":"frame_pointer"},{"ip":"0xea5745","module_index":0,"trust":"frame_pointer"},{"ip":"0xed18fe","module_index":0,"trust":"frame_pointer"},{"ip":"0x769cd828","module_index":2,"trust":"frame_pointer"},{"ip":"0x772c254c","module_index":3,"trust":"frame_pointer"},{"ip":"0x772c2520","module_index":3,"trust":"frame_pointer"}]}]}"#}
        =>
        {"reason":"EXCEPTION_BREAKPOINT","crashing_thread":0,"threads":[{"frames":[{"offset":"0x000000001260a399","module":"xul.dll","module_offset":"0x0000000002d5a399"},{"offset":"0x0000000010f86cae","module":"xul.dll","module_offset":"0x00000000016d6cae"},{"offset":"0x00000000145679de","module":"xul.dll","module_offset":"0x0000000004cb79de"},{"offset":"0x0000000014565241","module":"xul.dll","module_offset":"0x0000000004cb5241"},{"offset":"0x0000000010683f5e","module":"xul.dll","module_offset":"0x0000000000dd3f5e"},{"offset":"0x00000000106840b6","module":"xul.dll","module_offset":"0x0000000000dd40b6"},{"offset":"0x00000000110ac88f","module":"xul.dll","module_offset":"0x00000000017fc88f"},{"offset":"0x0000000011a0512a","module":"xul.dll","module_offset":"0x000000000215512a"},{"offset":"0x00000000114daa51","module":"xul.dll","module_offset":"0x0000000001c2aa51"},{"offset":"0x00000000110d2dcc","module":"xul.dll","module_offset":"0x0000000001822dcc"},{"offset":"0x0000000010f455d9","module":"xul.dll","module_offset":"0x00000000016955d9"},{"offset":"0x0000000010f58ab3","module":"xul.dll","module_offset":"0x00000000016a8ab3"},{"offset":"0x000000000f8dd87e","module":"xul.dll","module_offset":"0x000000000002d87e"},{"offset":"0x000000000f8dd7ee","module":"xul.dll","module_offset":"0x000000000002d7ee"},{"offset":"0x000000000f93dd04","module":"xul.dll","module_offset":"0x000000000008dd04"},{"offset":"0x000000000f93db96","module":"xul.dll","module_offset":"0x000000000008db96"},{"offset":"0x0000000014b75e6c","module":"xul.dll","module_offset":"0x00000000052c5e6c"},{"offset":"0x0000000011ed2160","module":"xul.dll","module_offset":"0x0000000002622160"},{"offset":"0x0000000011e74d1c","module":"xul.dll","module_offset":"0x00000000025c4d1c"},{"offset":"0x0000000011e749bb","module":"xul.dll","module_offset":"0x00000000025c49bb"},{"offset":"0x0000000011e74940","module":"xul.dll","module_offset":"0x00000000025c4940"},{"offset":"0x0000000000ea5745","module":"firefox.exe","module_offset":"0x0000000000005745"},{"offset":"0x0000000000ed18fe","module":"firefox.exe","module_offset":"0x00000000000318fe"},{"offset":"0x00000000769cd828","module":"kernel32.dll","module_offset":"0x000000000001d828"},{"offset":"0x00000000772c254c","module":"ntdll.dll","module_offset":"0x000000000002254c"},{"offset":"0x00000000772c2520","module":"ntdll.dll","module_offset":"0x0000000000022520"}]}]}
    }
}
