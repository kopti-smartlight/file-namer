import importlib.util
import sys
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "file-namer.py"
MODULE_SPEC = importlib.util.spec_from_file_location("file_namer", MODULE_PATH)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
file_namer = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = file_namer
MODULE_SPEC.loader.exec_module(file_namer)

BFileTarget = file_namer.BFileTarget
build_output_directory = file_namer.build_output_directory
build_output_filename = file_namer.build_output_filename
list_all_b_files = file_namer.list_all_b_files
parse_flat_b_file = file_namer.parse_flat_b_file


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"test")


def test_parse_flat_b_file_supports_sample_file_types() -> None:
    image = parse_flat_b_file(Path("0001_0006_image_12.png"))
    lumi = parse_flat_b_file(Path("0001_0006_lumi_12.png"))
    point_cloud = parse_flat_b_file(Path("0001_0006_pf_12.pf"))

    assert image is not None
    assert (image.stop, image.set_no, image.base, image.nn, image.ext) == (
        "0001",
        "0006",
        "image",
        "12",
        ".png",
    )
    assert lumi is not None and lumi.base == "lumi"
    assert point_cloud is not None
    assert (point_cloud.base, point_cloud.ext) == ("pf", ".pf")


def test_parse_flat_b_file_rejects_other_names() -> None:
    assert parse_flat_b_file(Path("image_12.png")) is None
    assert parse_flat_b_file(Path("0001_0006_image_1.png")) is None
    assert parse_flat_b_file(Path("0001_0006_image_123.png")) is None


def test_discovery_combines_legacy_and_recursive_flat_layouts(
    tmp_path: Path,
) -> None:
    legacy = tmp_path / "0003" / "0004" / "camera_12.png"
    legacy_flat_name = (
        tmp_path / "0003" / "0004" / "0001_0006_image_12.png"
    )
    single = tmp_path / "Single" / "0008" / "depth_12.png"
    flat_image = tmp_path / "converted" / "deep" / "0001_0006_image_12.png"
    flat_lumi = tmp_path / "converted" / "deep" / "0001_0006_lumi_12.png"
    flat_pf = tmp_path / "converted" / "deep" / "0001_0006_pf_12.pf"
    ignored = tmp_path / "converted" / "deep" / "notes.txt"

    for path in (
        legacy,
        legacy_flat_name,
        single,
        flat_image,
        flat_lumi,
        flat_pf,
        ignored,
    ):
        _touch(path)

    targets = list(list_all_b_files(tmp_path))
    by_path = {target.path: target for target in targets}

    assert len(targets) == 6
    assert sum(target.layout == "legacy" for target in targets) == 3
    assert sum(target.layout == "flat" for target in targets) == 3
    assert by_path[legacy].base == "camera"
    assert by_path[legacy_flat_name].layout == "legacy"
    assert by_path[single].mode == "single"
    assert (by_path[flat_image].stop, by_path[flat_image].set_no) == (
        "0001",
        "0006",
    )
    assert ignored not in by_path


def test_build_output_filename_preserves_existing_format() -> None:
    assert build_output_filename(
        "260806", "multi", "0001", "0006", "image", "H", "12", ".png"
    ) == "260806-0001-0006-image_H_12.png"
    assert build_output_filename(
        "260806", "single", None, "0006", "image", "M", "12", ".png"
    ) == "260806-0006-image_M_12.png"


def test_output_directory_depends_on_detected_layout(tmp_path: Path) -> None:
    b_date = tmp_path / "20260610"
    out_root = tmp_path / "output"
    flat_file = (
        b_date
        / "2_내조형"
        / "nested"
        / "0001_0006_image_12.png"
    )
    flat_target = parse_flat_b_file(flat_file)
    assert flat_target is not None

    legacy_target = BFileTarget(
        layout="legacy",
        mode="multi",
        stop="0001",
        set_no="0006",
        path=b_date / "0001" / "0006" / "image_12.png",
        nn="12",
        base="image",
        ext=".png",
    )

    assert build_output_directory(
        out_root, b_date, "260610", flat_target
    ) == out_root / "260610" / "2_내조형" / "nested"
    assert build_output_directory(
        out_root, b_date, "260610", legacy_target
    ) == out_root / "260610" / "12"
