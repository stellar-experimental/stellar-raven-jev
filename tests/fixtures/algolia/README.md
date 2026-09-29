# Extraction fixtures

These original fixtures describe a fictional Quillon paper archive.
They contain no source snapshots or real protocol facts.
The tests in `src/extract.rs` read both files without network access.

- `article.html` contains an article, noisy navigation, metadata, scripts, styles, hidden content, and code.
  Its navigation exceeds 30,000 characters. The test requires extracted text below that bound.
- `article.md` contains metadata, navigation, scripts, body text, and a fenced import.
  Tests require metadata removal and preservation of the import and body text.

Both formats test separate extraction paths.
