use super::*;

fn document(body: &str) -> String {
    format!(
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">{body}</assembly>"#
    )
}

fn utf16(text: &str, big_endian: bool) -> Vec<u8> {
    let mut bytes = if big_endian {
        vec![0xfe, 0xff]
    } else {
        vec![0xff, 0xfe]
    };
    for word in text.encode_utf16() {
        bytes.extend(if big_endian {
            word.to_be_bytes()
        } else {
            word.to_le_bytes()
        });
    }
    bytes
}

#[test]
fn common_application_metadata_is_closed_over_the_manifest_bytes() {
    let xml = document(
        r#"
      <assemblyIdentity name="Test.App" version="1.2.3.4" type="win32" processorArchitecture="*" publicKeyToken="0123456789abcdef" language="*"/>
      <assemblyIdentity name="App&amp;&#65;&#x1f680;"/>
      <description>App &amp; &#65; &#x1f680; &lt; &gt; &quot; &apos;<![CDATA[<text>]]></description>
      <noInherit/>
      <dependency optional="yes"><dependentAssembly><assemblyIdentity name="Microsoft.Windows.Common-Controls" version="6.0.0.0" type="win32"/></dependentAssembly></dependency>
      <trustInfo xmlns="urn:schemas-microsoft-com:asm.v3"><security><requestedPrivileges><requestedExecutionLevel level="asInvoker" uiAccess="false"/></requestedPrivileges></security></trustInfo>
      <trustInfo xmlns="urn:schemas-microsoft-com:asm.v2"><security><requestedPrivileges><requestedExecutionLevel level="asInvoker" uiAccess="false"/></requestedPrivileges></security></trustInfo>
      <compatibility xmlns="urn:schemas-microsoft-com:compatibility.v1"><application><supportedOS Id="{8e0f7a12-bfb3-4fe8-b9a5-48fd50a15a9a}"/><maxversiontested Id="10.0.19041.0"/></application></compatibility>
      <application xmlns="urn:schemas-microsoft-com:asm.v3"><windowsSettings>
        <dpiAware xmlns="http://schemas.microsoft.com/SMI/2005/WindowsSettings">true/pm</dpiAware>
        <dpiAwareness xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">PerMonitorV2, PerMonitor</dpiAwareness>
        <longPathAware xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">true</longPathAware>
      </windowsSettings></application>
    "#,
    );
    validate(xml.as_bytes()).unwrap();
    validate(br#"<v:assembly xmlns:v="urn:schemas-microsoft-com:asm.v1"><v:description name="bad"/></v:assembly>"#).unwrap_err();
    validate(br#"<v:assembly xmlns:v="urn:schemas-microsoft-com:asm.v1"><v:description>ok</v:description></v:assembly>"#).unwrap();
    validate(format!("<?xml version='1.0' encoding='UTF-8' standalone='yes'?>{xml}").as_bytes())
        .unwrap();
    validate(format!("<!--before-->{xml}<!--after-->").as_bytes()).unwrap();
    validate(format!("\r\n\t{xml}\r\n\t").as_bytes()).unwrap();
    validate(br#"<_v-1.x:assembly xmlns:_v-1.x="urn:schemas-microsoft-com:asm.v1"/>"#).unwrap();
}

#[test]
fn file_bearing_and_unmodeled_xml_shapes_are_refused() {
    for body in [
        r#"<file name="other.dll"/>"#,
        r#"<comClass clsid="x"/>"#,
        r#"<typelib tlbid="x"/>"#,
        r#"<assemblyIdentity name="&external;"/>"#,
        r#"<assemblyIdentity name="&#0;"/>"#,
        r#"<assemblyIdentity name="&#xD800;"/>"#,
        r#"<description>&external;</description>"#,
        r#"<description name="unsupported"/>"#,
        r#"<v:file xmlns:v="urn:schemas-microsoft-com:asm.v1" name="other"/>"#,
        r#"<xi:include xmlns:xi="http://www.w3.org/2001/XInclude" href="file.xml"/>"#,
        r#"<description xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="schema.xsd"/>"#,
        r#"<description xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="schema.xsd"/>"#,
        r#"<description xmlns:u="urn:unknown" u:href="other"/>"#,
        r#"<description xmlns:u="urn:unknown">unused namespace</description>"#,
        r#"<description xmlns:a="urn:schemas-microsoft-com:asm.v1" a:name="other"/>"#,
        r#"<unknown/>"#,
        r#"<description xmlns=""/>"#,
        r#"<?xml-stylesheet href="other.xsl"?>"#,
        r#"<?anything?>"#,
        r#"<!DOCTYPE description [<!ENTITY probe "text">]><description>&probe;</description>"#,
    ] {
        assert!(validate(document(body).as_bytes()).is_err(), "{body}");
    }
    for declaration in [
        "<!DOCTYPE assembly>",
        "<!DOCTYPE assembly [<!ENTITY probe 'text'>]>",
        "<!DOCTYPE assembly [<!ENTITY probe SYSTEM 'entity.txt'>]>",
        "<!DOCTYPE assembly SYSTEM 'external.dtd'>",
        "<!DOCTYPE assembly PUBLIC 'id' 'external.dtd'>",
        "<!DOCTYPE assembly [<!ENTITY % parameter SYSTEM 'external.dtd'>%parameter;]>",
    ] {
        assert!(
            validate(format!("{declaration}{}", document("")).as_bytes()).is_err(),
            "{declaration}"
        );
    }
}

#[test]
fn attribute_whitespace_and_embedded_quotes_remain_well_formed() {
    for separator in ["  ", "\t\t", "\r\n", " \t "] {
        let xml = format!(
            "<assembly{separator}xmlns=\"{ASM1}\"{separator}manifestVersion='1.0'>\
             <assemblyIdentity name=\"A'B\"{separator}version='1.0.0.0'/></assembly>"
        );
        validate(xml.as_bytes()).unwrap();
    }
    for body in [
        r#"<assemblyIdentity name="A'B"version="1.0.0.0"/>"#,
        r#"<assemblyIdentity name='A"B'version='1.0.0.0'/>"#,
    ] {
        assert!(validate(document(body).as_bytes()).is_err(), "{body}");
    }
}

#[test]
fn well_formedness_is_required_even_for_allowed_names() {
    for xml in [
        "",
        " \n",
        "<assembly/>",
        r#"<description xmlns="urn:schemas-microsoft-com:asm.v1"/>"#,
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1">"#,
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1"></description>"#,
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1" manifestVersion="2"/>"#,
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1"manifestVersion="1"/>"#,
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="<"/>"#,
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="&"/>"#,
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion=1/>"#,
        r#"<assembly xmlns="urn:schemas-microsoft-com:asm.v1"><description></description></assembly></assembly>"#,
        r#"<a:assembly xmlns:a="urn:schemas-microsoft-com:asm.v1"></assembly>"#,
        r#"<0:assembly xmlns:0="urn:schemas-microsoft-com:asm.v1"/>"#,
        r#"<a!:assembly xmlns:a!="urn:schemas-microsoft-com:asm.v1"/>"#,
    ] {
        assert!(validate(xml.as_bytes()).is_err(), "{xml}");
    }
    for body in [
        "<!--bad--comment-->",
        "<description>]]></description>",
        "<description>&</description>",
        "<description>&#0;</description>",
        "<description>&#xD800;</description>",
        "<description>&#x110000;</description>",
        "<description>&#-1;</description>",
        "<description>&#x;</description>",
        "<description>&bogus;</description>",
        "<description>\0</description>",
        "<description>\u{fffe}</description>",
        "<?xml version='1.0'?>",
    ] {
        assert!(validate(document(body).as_bytes()).is_err(), "{body:?}");
    }
    for outside in ["text", "&amp;", "<![CDATA[text]]>"] {
        assert!(validate(format!("{outside}{}", document("")).as_bytes()).is_err());
        assert!(validate(format!("{}{outside}", document("")).as_bytes()).is_err());
    }
    assert!(validate(format!("{}{}", document(""), document("")).as_bytes()).is_err());
}

#[test]
fn encoding_and_declaration_agree_and_preserve_unicode() {
    let xml = document("<description>π 🚀</description>");
    validate(xml.as_bytes()).unwrap();
    validate(&[b"\xef\xbb\xbf".as_slice(), xml.as_bytes()].concat()).unwrap();
    for big_endian in [false, true] {
        validate(&utf16(&xml, big_endian)).unwrap();
        for encoding in ["UTF-16", if big_endian { "UTF-16BE" } else { "UTF-16LE" }] {
            validate(&utf16(
                &format!("<?xml version='1.0' encoding='{encoding}'?>{xml}"),
                big_endian,
            ))
            .unwrap();
        }
        for encoding in ["UTF-8", if big_endian { "UTF-16LE" } else { "UTF-16BE" }] {
            assert!(
                validate(&utf16(
                    &format!("<?xml version='1.0' encoding='{encoding}'?>{xml}"),
                    big_endian
                ))
                .is_err()
            );
        }
        let mut odd = utf16(&xml, big_endian);
        odd.pop();
        assert!(validate(&odd).is_err());
    }
    for declaration in [
        "<?xml version='1.1'?>",
        "<?xml encoding='UTF-8'?>",
        "<?xml version='1.0' version='1.0'?>",
        "<?xml version='1.0' encoding='UTF-8' encoding='UTF-8'?>",
        "<?xml version='1.0' standalone='yes' standalone='yes'?>",
        "<?xml version='1.0' encoding='UTF-16'?>",
        "<?xml version='1.0' encoding='windows-1252'?>",
        "<?xml version='1.0' standalone='maybe'?>",
        "<?xml version='1.0' other='x'?>",
        "<?xml version='1.0' standalone='yes' encoding='UTF-8'?>",
        "<?xml version='1.0'encoding='UTF-8'?>",
        " <?xml version='1.0'?>",
        "<!--x--><?xml version='1.0'?>",
    ] {
        assert!(
            validate(format!("{declaration}{xml}").as_bytes()).is_err(),
            "{declaration}"
        );
    }
    for bytes in [
        vec![0xff],
        vec![0xff, 0xfe, 0x00, 0xd8],
        vec![0xfe, 0xff, 0xd8, 0x00],
        vec![0, 0, 0xfe, 0xff],
    ] {
        assert!(validate(&bytes).is_err(), "{bytes:?}");
    }
    assert!(
        validate(&utf16(&xml, false)[2..]).is_err(),
        "BOM required for UTF-16"
    );
}

#[test]
fn original_byte_limit_includes_bom_and_trailing_whitespace() {
    let mut bytes = document("").into_bytes();
    bytes.resize(1_048_576, b' ');
    validate(&bytes).unwrap();
    bytes.push(b' ');
    assert!(validate(&bytes).is_err());
    let mut unicode = utf16(&document(""), false);
    while unicode.len() < 1_048_576 {
        unicode.extend([b' ', 0]);
    }
    validate(&unicode).unwrap();
    unicode.extend([b' ', 0]);
    assert!(validate(&unicode).is_err());
}

#[test]
fn nesting_and_element_count_have_independent_limits() {
    let nested = |levels: usize| {
        document(&format!(
            "{}{}",
            "<description>".repeat(levels),
            "</description>".repeat(levels)
        ))
    };
    validate(nested(31).as_bytes()).unwrap();
    assert!(validate(nested(32).as_bytes()).is_err());
    validate(document(&"<description/>".repeat(4095)).as_bytes()).unwrap();
    assert!(validate(document(&"<description/>".repeat(4096)).as_bytes()).is_err());
}

#[test]
fn attribute_and_namespace_counts_have_independent_limits() {
    let attributes = |extra: usize| {
        let attrs = (0..extra)
            .map(|i| format!(r#" xmlns:p{i}="urn:schemas-microsoft-com:asm.v1""#))
            .collect::<String>();
        document(&format!(
            r#"<assemblyIdentity name="App" version="1" type="win32" processorArchitecture="*" publicKeyToken="*" language="*"{attrs}/>"#
        ))
    };
    validate(attributes(26).as_bytes()).unwrap();
    assert!(validate(attributes(27).as_bytes()).is_err());
    let nested = |count: usize| {
        let body = (0..count).map(|i| format!(r#"<description xmlns:a{i}="urn:schemas-microsoft-com:asm.v1" xmlns:b{i}="urn:schemas-microsoft-com:asm.v1">"#)).collect::<String>();
        document(&format!("{body}{}", "</description>".repeat(count)))
    };
    validate(nested(15).as_bytes()).unwrap();
    assert!(validate(nested(16).as_bytes()).is_err());
}
