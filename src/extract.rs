//! Extract visible text from HTML and published Markdown.
use std::collections::HashMap;

pub(crate) fn markdown_for_scoring(text: &str) -> Option<String> {
    let mut fence: Option<(char, usize)> = None;
    let mut hidden: Option<&str> = None;
    let mut output = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        let lower = trimmed.to_ascii_lowercase();
        if let Some(tag) = hidden {
            if lower.contains(&format!("</{tag}>")) {
                hidden = None;
            }
            continue;
        }
        let marker = trimmed.chars().next().unwrap_or(' ');
        let marker_count = trimmed.chars().take_while(|c| *c == marker).count();
        if matches!(marker, '`' | '~') && marker_count >= 3 {
            match fence {
                None => fence = Some((marker, marker_count)),
                Some((open, count)) if marker == open && marker_count >= count => fence = None,
                _ => (),
            }
            output.push(line);
            continue;
        }
        if fence.is_some() {
            output.push(line);
            continue;
        }
        if let Some(tag) = ["head", "script", "style", "nav", "aside", "footer"]
            .into_iter()
            .find(|tag| {
                lower.starts_with(&format!("<{tag}>")) || lower.starts_with(&format!("<{tag} "))
            })
        {
            if !lower.contains(&format!("</{tag}>")) {
                hidden = Some(tag);
            }
            continue;
        }
        if trimmed
            .strip_prefix('<')
            .and_then(|tail| tail.chars().next())
            .is_some_and(|c| c.is_ascii_uppercase())
            || trimmed.starts_with("import ")
            || trimmed.starts_with("export ")
        {
            return None;
        }
        output.push(line);
    }
    let result = output.join("\n");
    if result.trim().is_empty() {
        None
    } else {
        Some(result)
    }
}
pub(crate) fn unresolved_markdown_component(text: &str) -> bool {
    markdown_for_scoring(text).is_none()
}

fn decode_entities(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];
        // Look for the ';' in the next 16 bytes only, so a run of '&' stays linear.
        let window = &rest.as_bytes()[..rest.len().min(17)];
        let Some(end) = window.iter().position(|b| *b == b';') else {
            out.push('&');
            rest = &rest[1..];
            continue;
        };
        let entity = &rest[1..end];
        let value = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" | "#39" => Some('\''),
            "nbsp" => Some(' '),
            "ndash" => Some('–'),
            "mdash" => Some('—'),
            "hellip" => Some('…'),
            _ => entity
                .strip_prefix("#x")
                .or_else(|| entity.strip_prefix("#X"))
                .and_then(|n| u32::from_str_radix(n, 16).ok())
                .or_else(|| entity.strip_prefix('#').and_then(|n| n.parse().ok()))
                .and_then(char::from_u32),
        };
        if let Some(value) = value {
            out.push(value);
        } else {
            out.push_str(&rest[..=end]);
        }
        rest = &rest[end + 1..];
    }
    out.push_str(rest);
    out.replace('\u{200b}', "")
}
fn tag_attributes(tag: &str) -> HashMap<String, String> {
    let mut attrs = HashMap::new();
    let bytes = tag.as_bytes();
    let mut at = 0;
    while at < bytes.len() && !bytes[at].is_ascii_whitespace() {
        at += 1;
    }
    while at < bytes.len() {
        while at < bytes.len() && (bytes[at].is_ascii_whitespace() || bytes[at] == b'/') {
            at += 1;
        }
        let start = at;
        while at < bytes.len() && !bytes[at].is_ascii_whitespace() && bytes[at] != b'=' {
            at += 1;
        }
        if at == start {
            break;
        }
        let name = tag[start..at].to_ascii_lowercase();
        while at < bytes.len() && bytes[at].is_ascii_whitespace() {
            at += 1;
        }
        let mut value = String::new();
        if at < bytes.len() && bytes[at] == b'=' {
            at += 1;
            while at < bytes.len() && bytes[at].is_ascii_whitespace() {
                at += 1;
            }
            let quote = bytes.get(at).copied().filter(|b| *b == b'\'' || *b == b'"');
            if quote.is_some() {
                at += 1;
            }
            let start = at;
            while at < bytes.len()
                && match quote {
                    Some(q) => bytes[at] != q,
                    None => !bytes[at].is_ascii_whitespace(),
                }
            {
                at += 1;
            }
            value = tag[start..at].to_owned();
            if quote.is_some() && at < bytes.len() {
                at += 1;
            }
        }
        attrs.insert(name, value);
    }
    attrs
}
/// Open elements past this depth, or tags past this count, stop extraction: the page is refused.
const HTML_MAX_DEPTH: usize = 4_096;
const HTML_MAX_TAGS: usize = 250_000;
/// The scopes whose text the extractor collects, in order of preference.
const HTML_SCOPES: [&str; 3] = ["article", "main", "body"];

/// The open elements, with counts kept as elements open and close, so every question the
/// extractor asks about them takes constant time.
#[derive(Default)]
struct OpenElements {
    stack: Vec<(String, bool)>,
    hidden: usize,
    scopes: [usize; 3],
    names: HashMap<String, usize>,
}

impl OpenElements {
    fn push(&mut self, name: String, hidden: bool) {
        self.hidden += usize::from(hidden);
        if let Some(i) = HTML_SCOPES.iter().position(|s| *s == name) {
            self.scopes[i] += 1;
        }
        *self.names.entry(name.clone()).or_default() += 1;
        self.stack.push((name, hidden));
    }

    /// Close the innermost open element named `name` and every element inside it. Each element
    /// is removed once, so closing stays linear over the page.
    fn close(&mut self, name: &str) {
        if self.names.get(name).is_none_or(|n| *n == 0) {
            return;
        }
        while let Some((open, hidden)) = self.stack.pop() {
            self.hidden -= usize::from(hidden);
            if let Some(i) = HTML_SCOPES.iter().position(|s| *s == open) {
                self.scopes[i] -= 1;
            }
            if let Some(n) = self.names.get_mut(&open) {
                *n -= 1;
            }
            if open == name {
                break;
            }
        }
    }

    fn visible(&self) -> bool {
        self.hidden == 0
    }
}

pub(crate) fn html_article_text(text: &str) -> Option<(String, &'static str)> {
    // Bounded lexical HTML extraction. No scripts, CSS, or network resources execute. Work is
    // linear in the page size.
    let lower = text.to_ascii_lowercase();
    let mut open = OpenElements::default();
    let mut tags = 0usize;
    let mut buffers = [String::new(), String::new(), String::new()];
    let mut at = 0;
    while at < text.len() {
        if !text[at..].starts_with('<') {
            let end = text[at..].find('<').map(|n| at + n).unwrap_or(text.len());
            if open.visible() && open.scopes.iter().any(|n| *n > 0) {
                let chunk = decode_entities(&text[at..end]);
                for (i, buffer) in buffers.iter_mut().enumerate() {
                    if open.scopes[i] > 0 {
                        buffer.push_str(&chunk);
                    }
                }
            }
            at = end;
            continue;
        }
        if text[at..].starts_with("<!--") {
            at = text[at + 4..]
                .find("-->")
                .map(|n| at + 4 + n + 3)
                .unwrap_or(text.len());
            continue;
        }
        let mut end = at + 1;
        let mut quote = None;
        for byte in text.as_bytes().iter().skip(at + 1) {
            if let Some(q) = quote {
                if *byte == q {
                    quote = None;
                }
            } else if *byte == b'\'' || *byte == b'"' {
                quote = Some(*byte);
            } else if *byte == b'>' {
                break;
            }
            end += 1;
        }
        if end >= text.len() {
            break;
        }
        let raw = text[at + 1..end].trim();
        at = end + 1;
        let closing = raw.starts_with('/');
        let tag = raw
            .trim_start_matches('/')
            .split(|c: char| c.is_ascii_whitespace() || c == '/')
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        if tag.is_empty() || tag.starts_with(['!', '?']) {
            continue;
        }
        tags += 1;
        if tags > HTML_MAX_TAGS {
            return None;
        }
        if matches!(tag.as_str(), "script" | "style" | "noscript" | "template") && !closing {
            at = lower[at..]
                .find(&format!("</{tag}"))
                .map(|n| at + n)
                .unwrap_or(text.len());
            continue;
        }
        let block = matches!(
            tag.as_str(),
            "p" | "div"
                | "li"
                | "h1"
                | "h2"
                | "h3"
                | "h4"
                | "h5"
                | "h6"
                | "pre"
                | "br"
                | "tr"
                | "section"
                | "table"
                | "article"
        );
        if block && open.visible() {
            for (i, buffer) in buffers.iter_mut().enumerate() {
                if open.scopes[i] > 0 {
                    buffer.push('\n');
                }
            }
        }
        if closing {
            open.close(&tag);
            continue;
        }
        let attrs = tag_attributes(raw);
        let style = attrs
            .get("style")
            .map(|s| s.to_ascii_lowercase().replace(' ', ""))
            .unwrap_or_default();
        let class = attrs.get("class").map(String::as_str).unwrap_or("");
        let hidden = matches!(
            tag.as_str(),
            "nav" | "aside" | "footer" | "head" | "svg" | "button" | "form"
        ) || (tag == "header" && open.scopes[0] + open.scopes[1] == 0)
            || attrs.contains_key("hidden")
            || attrs
                .get("aria-hidden")
                .is_some_and(|v| v.eq_ignore_ascii_case("true"))
            || style.contains("display:none")
            || style.contains("visibility:hidden")
            || class.split_ascii_whitespace().any(|c| {
                matches!(
                    c,
                    "sr-only" | "visually-hidden" | "table-of-contents" | "theme-doc-toc-mobile"
                )
            });
        // An empty <time> element is filled in by page scripts. Its machine-readable value is the
        // only copy of the date, so it becomes text.
        if tag == "time" && !hidden && open.visible() {
            if let Some(value) = attrs.get("datetime").filter(|v| !v.trim().is_empty()) {
                let empty = lower[at..].trim_start().starts_with("</time");
                if empty {
                    for (i, buffer) in buffers.iter_mut().enumerate() {
                        if open.scopes[i] > 0 {
                            buffer.push(' ');
                            buffer.push_str(value.trim());
                            buffer.push(' ');
                        }
                    }
                }
            }
        }
        if !raw.ends_with('/')
            && !matches!(
                tag.as_str(),
                "area"
                    | "base"
                    | "br"
                    | "col"
                    | "embed"
                    | "hr"
                    | "img"
                    | "input"
                    | "link"
                    | "meta"
                    | "param"
                    | "source"
                    | "track"
                    | "wbr"
            )
        {
            if open.stack.len() >= HTML_MAX_DEPTH {
                return None;
            }
            open.push(tag, hidden);
        }
    }
    let [article, main, body] = buffers;
    for (raw, scope) in [
        (article, "article_visible_text"),
        (main, "main_visible_text"),
        (body, "body_visible_text"),
    ] {
        let cleaned = raw
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        if !cleaned.is_empty() {
            return Some((cleaned, scope));
        }
    }
    None
}

/// The original page for one hit. Failures are returned, so concurrent reads can be applied in
/// hit order.
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn saved_state_archival_html_excludes_scripts_and_navigation() {
        let raw = include_str!("../tests/fixtures/algolia/article.html");
        assert!(raw.chars().count() > 30000);
        let (text, scope) = html_article_text(raw).unwrap();
        eprintln!(
            "Quillon Archive: raw_html_chars={} extracted_article_chars={}",
            raw.chars().count(),
            text.chars().count()
        );
        assert!(
            text.chars().count() < 30000,
            "Scoring text contains {} characters",
            text.chars().count()
        );
        assert!(!text.contains("<script"));
        assert!(!text.contains("Docs sidebar"));
        assert!(text.contains("Quillon Archive"));
        assert!(text.contains("restore"));
        assert_eq!(scope, "article_visible_text");
    }
    #[test]
    fn saved_state_archival_markdown_accepts_imports_in_code_fences() {
        let raw = include_str!("../tests/fixtures/algolia/article.md");
        assert!(
            !unresolved_markdown_component(raw),
            "Code examples are not unresolved page components"
        );
    }
    #[test]
    fn an_empty_time_element_keeps_its_machine_readable_date() {
        let raw = r#"<html><body><article><p>Publishing date</p><time dateTime="2024-06-18T11:00:00.000Z" class="x"></time><p>Posted <time datetime="2020-01-01">January 1</time></p></article></body></html>"#;
        let (text, _) = html_article_text(raw).unwrap();
        assert!(
            text.contains("Publishing date\n2024-06-18T11:00:00.000Z"),
            "{text}"
        );
        assert!(text.contains("Posted January 1") && !text.contains("2020-01-01"));
    }
    #[test]
    fn hostile_html_extracts_in_linear_time_or_is_refused() {
        let timed = |html: String| {
            let started = std::time::Instant::now();
            let result = html_article_text(&html);
            (result, started.elapsed())
        };
        // Deep nesting past the depth cap is refused at once.
        let deep = format!("<body>{}</body>", "<div>a".repeat(160_000));
        assert!(deep.len() < 2 * 1024 * 1024);
        let (result, elapsed) = timed(deep);
        assert!(result.is_none());
        assert!(elapsed < std::time::Duration::from_secs(2), "{elapsed:?}");
        // Nesting under the cap with many text runs, stray closing tags, and a run of '&'.
        let wide = format!(
            "<body>{}{}{}<p>{}</p></body>",
            "<div>".repeat(4_000),
            "a<br>".repeat(100_000),
            "</span>".repeat(50_000),
            "&".repeat(200_000)
        );
        let (result, elapsed) = timed(wide);
        let (text, scope) = result.expect("text under the caps is kept");
        assert_eq!(scope, "body_visible_text");
        assert!(text.starts_with('a') && text.ends_with('&'));
        assert!(elapsed < std::time::Duration::from_secs(2), "{elapsed:?}");
        // Too many tags are refused.
        let (result, _) = timed(format!("<body>{}</body>", "<br>".repeat(HTML_MAX_TAGS)));
        assert!(result.is_none());
    }
    #[test]
    fn extraction_removes_hidden_content_and_keeps_article_heading() {
        let raw = r#"<html><body><nav>Menu</nav><main><article><header><h1>Title</h1></header><p>A &amp; B</p><script>secret &lt;tag&gt;</script><style>.x{}</style><div hidden>Hidden</div><aside>Sidebar</aside><p>Restored &#60;code&#62;</p></article></main><footer>Footer</footer></body></html>"#;
        let (text, scope) = html_article_text(raw).unwrap();
        assert_eq!(scope, "article_visible_text");
        assert!(text.contains("Title"));
        assert!(text.contains("A & B"));
        assert!(text.contains("Restored <code>"));
        for noise in ["Menu", "secret", "Hidden", "Sidebar", "Footer", ".x"] {
            assert!(!text.contains(noise));
        }
    }
    #[test]
    fn markdown_scoring_removes_metadata_without_removing_code_examples() {
        let raw = include_str!("../tests/fixtures/algolia/article.md");
        let text = markdown_for_scoring(raw).unwrap();
        eprintln!(
            "Quillon Archive: raw_markdown_chars={} scoring_markdown_chars={}",
            raw.chars().count(),
            text.chars().count()
        );
        assert!(!text.contains("<head>"));
        assert!(text.contains("import {"));
        assert!(text.contains("restore"));
    }
    #[test]
    fn generated_rpc_markdown_requires_the_original_html() {
        assert!(unresolved_markdown_component(
            "<RpcMethod\n method={rpcSpec.methods[0]}\n/>"
        ));
        assert!(!unresolved_markdown_component(
            "# Heading\n\nComplete prose."
        ));
    }
}
