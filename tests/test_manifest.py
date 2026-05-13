import base64

from drive_to_s3.manifest import ManifestEntry, load_manifest


def test_load_manifest(tmp_path):
    m = tmp_path / "manifest.yml"
    m.write_text(
        """pipelines:
  - name: p1
    config_path: config/a.yml
  - name: p2
    config_path: config/b.yml
",
        encoding="utf-8",
    )

    entries = load_manifest(str(m))
    assert entries == [
        ManifestEntry(name="p1", config_path="config/a.yml"),
        ManifestEntry(name="p2", config_path="config/b.yml"),
    ]
