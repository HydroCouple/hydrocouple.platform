"""swmm.simulation@2 overrides: which fields may be written, and what is recorded."""

from __future__ import annotations

import pytest

from conftest import DATA
from hydrocouple_platform.runner import RunnerFailure
from hydrocouple_platform.workflows import get_workflow

pytestmark = pytest.mark.engine


@pytest.fixture
def solver(artifact_dir):
    engine = pytest.importorskip("openswmm.engine")
    s = engine.Solver(str(DATA / "site_drainage_example.inp"), str(artifact_dir / "m.rpt"),
                      str(artifact_dir / "m.out"))
    s.open()
    yield s
    s.close()
    s.destroy()


def test_writable_scalar_element_fields_are_written_with_their_base_values(solver):
    from hydrocouple_platform.runner.swmm import apply_overrides

    applied = apply_overrides(solver, [
        {"path": "subcatchment.width", "id": "S1", "value": 300},
        {"path": "link.roughness", "id": "C2", "value": 0.02},
        {"path": "link.flap_gate", "id": "C2", "value": True},
    ])
    assert applied == [
        {"path": "subcatchment.width", "id": "S1", "value": 300, "base_value": 1587.0,
         "unit": "ft"},
        {"path": "link.roughness", "id": "C2", "value": 0.02, "base_value": 0.016,
         "unit": "Manning's n"},
        {"path": "link.flap_gate", "id": "C2", "value": True, "base_value": False, "unit": None},
    ]
    assert solver.subcatchments["S1"].width == 300.0
    assert solver.links["C2"].roughness == 0.02 and solver.links["C2"].flap_gate is True
    solver.initialize()  # the engine accepts every write before it initializes


@pytest.mark.parametrize("override, reason", [
    ({"path": "link.stats.max_flow", "id": "C2", "value": 1.0}, "only writable"),
    ({"path": "link.from_node", "id": "C2", "value": 1}, "only writable"),
    ({"path": "climate.latitude", "id": "x", "value": 40.0}, "only writable"),
    ({"path": "link.no_such_field", "id": "C2", "value": 1.0}, "not an engine field"),
    ({"path": "link.barrels", "id": "C2", "value": 2.5}, "needs a int value"),
    ({"path": "link.flap_gate", "id": "C2", "value": 1}, "needs a bool value"),
    ({"path": "link.roughness", "id": "NO_SUCH_LINK", "value": 0.02}, "override 1"),
])
def test_anything_else_is_invalid_input(solver, override, reason):
    from hydrocouple_platform.runner import EXIT_INVALID_INPUT
    from hydrocouple_platform.runner.swmm import apply_overrides

    with pytest.raises(RunnerFailure) as err:
        apply_overrides(solver, [override])
    assert err.value.exit_code == EXIT_INVALID_INPUT and reason in str(err.value)


def test_version_2_takes_overrides_and_version_1_does_not():
    from hydrocouple_platform.contracts.errors import PlatformError

    overrides = [{"path": "subcatchment.width", "id": "S1", "value": 300}]
    assert get_workflow("swmm.simulation", "2").validate_parameters(
        {"overrides": overrides})["overrides"] == overrides
    assert get_workflow("swmm.simulation", "2").validate_parameters({})["overrides"] == []
    with pytest.raises(PlatformError, match="invalid"):
        get_workflow("swmm.simulation", "1").validate_parameters({"overrides": overrides})
    with pytest.raises(PlatformError, match="invalid"):
        get_workflow("swmm.simulation", "2").validate_parameters(
            {"overrides": [{"path": "subcatchment.width", "id": "S1", "value": "wide"}]})
