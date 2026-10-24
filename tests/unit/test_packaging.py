"""Packaging: reference scanning, rewriting, safety checks and the composition."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import jsonschema
import pytest

from conftest import DATA
from hydrocouple_platform.packaging import PackagingError, package_swmm_model
from hydrocouple_platform.packaging import inp as inpscan
from hydrocouple_platform.runner.core import load_schema
from hydrocouple_platform.storage import sha256_file

SAMPLE = """[TITLE]
;; comment with FILE "not-a-ref.dat"
[FILES]
USE HOTSTART   "start state.hsf"
SAVE HOTSTART  end.hsf
[RAINGAGES]
G1  INTENSITY 0:05 1.0 FILE "rain gauge.dat" STA1 IN
[TIMESERIES]
TS1 FILE inflow.csv:2   ; column decorator
TS2 0:00 1.0
[TEMPERATURE]
FILE climate.txt
[2D_MESH_FILE]
FILE meshes/my mesh.2dm
[PLUGINS]
libcustom.so
[PROCESS_COMPONENTS]
pc1 type=reactor config="cfg/reactor.json"
"""


class TestScanner:
    def test_finds_every_declared_reference(self):
        refs = {(r.section, r.kind.value, r.text) for r in inpscan.scan(SAMPLE)}
        assert refs == {
            ("FILES", "input", "start state.hsf"),
            ("FILES", "output", "end.hsf"),
            ("RAINGAGES", "input", "rain gauge.dat"),
            ("TIMESERIES", "input", "inflow.csv:2"),
            ("TEMPERATURE", "input", "climate.txt"),
            ("2D_MESH_FILE", "input", "meshes/my mesh.2dm"),
            ("PLUGINS", "plugin", "libcustom.so"),
            ("PROCESS_COMPONENTS", "input", "cfg/reactor.json"),
        }

    def test_rewrite_changes_only_the_path_and_keeps_quotes(self):
        refs = [r for r in inpscan.scan(SAMPLE) if r.section in ("RAINGAGES", "TEMPERATURE")]
        out = inpscan.rewrite(SAMPLE, [(refs[0], "rain/gauge new.dat"),
                                       (refs[1], "climate dir/climate.txt")])
        assert 'FILE "rain/gauge new.dat" STA1 IN' in out
        assert 'FILE "climate dir/climate.txt"' in out
        assert out.count("\n") == SAMPLE.count("\n")


class TestPackage:
    def test_inline_model_packages_as_one_component(self, artifact_dir):
        pkg = package_swmm_model(DATA / "site_drainage_example.inp", artifact_dir / "pkg")
        paths = {f.path for f in pkg.manifest.files}
        assert paths == {"composition.json", "model/model.inp"}
        composition = json.loads((artifact_dir / "pkg" / "composition.json").read_text())
        jsonschema.validate(composition, load_schema("composition-1.1.json"))
        assert composition["components"][0]["info"]["component_info_id"] == \
            "org.hydrocouple.openswmm.engine"

    def test_external_rain_is_included_and_original_untouched(self, artifact_dir):
        src = artifact_dir / "src"
        shutil.copytree(DATA / "external_rain", src)
        before = sha256_file(src / "model.inp")
        pkg = package_swmm_model(src / "model.inp", artifact_dir / "pkg")
        files = {f.path: f for f in pkg.manifest.files}
        assert "model/rain/2-yr storm.dat" in files
        assert files["model/rain/2-yr storm.dat"].sha256 == sha256_file(
            src / "rain" / "2-yr storm.dat")
        assert sha256_file(src / "model.inp") == before
        packaged = (artifact_dir / "pkg" / "model" / "model.inp").read_text()
        assert '2-yr             FILE "rain/2-yr storm.dat"' in packaged
        assert pkg.manifest.digest() == pkg.manifest.digest()

    def test_reference_outside_the_folder_is_copied_under_external(self, artifact_dir):
        outside = artifact_dir / "shared" / "storm.dat"
        outside.parent.mkdir()
        shutil.copyfile(DATA / "external_rain" / "rain" / "2-yr storm.dat", outside)
        src = artifact_dir / "src"
        shutil.copytree(DATA / "external_rain", src)
        text = (src / "model.inp").read_text().replace('"rain/2-yr storm.dat"', f'"{outside}"')
        (src / "model.inp").write_text(text)
        pkg = package_swmm_model(src / "model.inp", artifact_dir / "pkg")
        assert "model/external/0-storm.dat" in {f.path for f in pkg.manifest.files}
        assert any("outside the model folder" in f for f in pkg.manifest.findings)

    def test_missing_file_and_plugin_are_refused_together(self, artifact_dir):
        model = artifact_dir / "m.inp"
        model.write_text(SAMPLE)
        with pytest.raises(PackagingError) as err:
            package_swmm_model(model, artifact_dir / "pkg")
        joined = " | ".join(err.value.problems)
        assert "native plugins" in joined and "does not exist" in joined

    @pytest.mark.skipif(not hasattr(os, "symlink"), reason="needs symbolic links")
    def test_symlink_escape_is_refused(self, artifact_dir):
        secret = artifact_dir / "outside.dat"
        secret.write_text("0:00 1.0\n")
        src = artifact_dir / "src"
        shutil.copytree(DATA / "external_rain", src)
        (src / "rain" / "2-yr storm.dat").unlink()
        try:
            (src / "rain" / "2-yr storm.dat").symlink_to(secret)
        except OSError:
            pytest.skip("this filesystem cannot create symbolic links")
        with pytest.raises(PackagingError, match="escapes the model folder"):
            package_swmm_model(src / "model.inp", artifact_dir / "pkg")

    def test_package_paths_are_posix_relative(self, artifact_dir):
        pkg = package_swmm_model(DATA / "site_drainage_example.inp", artifact_dir / "pkg")
        for f in pkg.manifest.files:
            assert not Path(f.path).is_absolute() and "\\" not in f.path


class TestStore:
    def test_conditional_create_never_replaces(self, artifact_dir):
        from hydrocouple_platform.storage import LocalStore, ObjectExists

        store = LocalStore(artifact_dir / "store")
        first, second = artifact_dir / "a.txt", artifact_dir / "b.txt"
        first.write_text("first")
        second.write_text("second")
        store.put_file("runs/x/attempt.json", first, if_absent=True)
        with pytest.raises(ObjectExists):
            store.put_file("runs/x/attempt.json", second, if_absent=True)
        assert store.stat("runs/x/attempt.json") == (5, sha256_file(first))
        with pytest.raises(ValueError):
            store.path("../outside")
