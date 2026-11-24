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
to_yymmdd = file_namer.to_yymmdd
find_a_date_dir = file_namer.find_a_date_dir
looks_like_raw_date_dir = file_namer.looks_like_raw_date_dir


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


def test_output_directory_keeps_b_structure_unless_camera_sorting(tmp_path: Path) -> None:
    # 기본(체크 해제): 두 형식 모두 B 안의 폴더 구조 그대로. 체크: 카메라번호별 (예전 지점 폴더 형식 동작)
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
    ) == out_root / "260610" / "0001" / "0006"

    assert build_output_directory(
        out_root, b_date, "260610", flat_target, by_camera=True
    ) == out_root / "260610" / "12"
    assert build_output_directory(
        out_root, b_date, "260610", legacy_target, by_camera=True
    ) == out_root / "260610" / "12"


def test_output_directory_keeps_single_folder(tmp_path: Path) -> None:
    b_date = tmp_path / "261014"
    single_target = BFileTarget(
        layout="legacy",
        mode="single",
        stop=None,
        set_no="0008",
        path=b_date / "Single" / "0008" / "image_12.png",
        nn="12",
        base="image",
        ext=".png",
    )
    assert build_output_directory(
        tmp_path / "out", b_date, "261014", single_target
    ) == tmp_path / "out" / "261014" / "Single" / "0008"


def test_to_yymmdd_reads_date_anywhere_in_folder_name() -> None:
    # 예전에는 이름의 마지막 6글자만 봐서 '3-a.20261014-test-autorefined' 같은 B 폴더에서 멈췄다
    assert to_yymmdd("261014") == "261014"
    assert to_yymmdd("20261014") == "261014"
    assert to_yymmdd("3-a.20261014-test-autorefined") == "261014"
    assert to_yymmdd("3-b.20261014-test-autorefined-조명종류별분류") == "261014"
    assert to_yymmdd("1.20261014-test-원시") == "261014"
    assert to_yymmdd("자동정제_261014_v2") == "261014"
    assert to_yymmdd("20261014_261014") == "261014"  # 같은 날짜가 두 번이면 괜찮다


def test_to_yymmdd_rejects_missing_or_ambiguous_dates() -> None:
    assert to_yymmdd("autorefined") is None
    assert to_yymmdd("0001") is None
    assert to_yymmdd("20261014-20261015") is None  # 서로 다른 날짜 둘
    assert to_yymmdd("261399") is None  # 13월은 날짜가 아님
    assert to_yymmdd("120261014") is None  # 숫자 9자리 덩어리


def _make_raw_date_dir(date_dir: Path) -> None:
    _touch(date_dir / "0001" / "gps.db")
    _touch(date_dir / "0001" / "0001" / "Focus 10_Middle_image_02.png")


def _make_converted_date_dir(date_dir: Path) -> None:
    _touch(date_dir / "0001" / "gps.db")
    _touch(date_dir / "0001" / "0001" / "image_02.png")


def test_looks_like_raw_date_dir(tmp_path: Path) -> None:
    _make_raw_date_dir(tmp_path / "raw")
    _make_converted_date_dir(tmp_path / "converted")
    _touch(tmp_path / "single_raw" / "Single" / "0003" / "Focus 10_High_image_12.png")
    assert looks_like_raw_date_dir(tmp_path / "raw")
    assert looks_like_raw_date_dir(tmp_path / "single_raw")
    assert not looks_like_raw_date_dir(tmp_path / "converted")
    assert not looks_like_raw_date_dir(tmp_path / "없음")


def test_find_a_date_dir_prefers_raw_among_same_date_folders(tmp_path: Path) -> None:
    # 시험 폴더처럼 같은 날짜 이름의 단계별 폴더가 한곳에 있을 때 원시를 고른다
    _make_converted_date_dir(tmp_path / "2.20261014-test-converted")
    _make_converted_date_dir(tmp_path / "3-a.20261014-test-autorefined")
    _make_raw_date_dir(tmp_path / "원시_20261014")  # 이름순으로는 뒤
    _touch(tmp_path / "261014" / "광고조명" / "x.png")  # 이름만 날짜인 자동정제 결과
    assert find_a_date_dir(tmp_path, "261014") == tmp_path / "원시_20261014"


def test_find_a_date_dir_keeps_plain_date_layout(tmp_path: Path) -> None:
    # NAS 01_원시데이터/동적 처럼 날짜 이름 폴더만 있는 경우는 예전과 같다
    _make_raw_date_dir(tmp_path / "20261014")
    _make_raw_date_dir(tmp_path / "20261015")
    assert find_a_date_dir(tmp_path, "261014") == tmp_path / "20261014"
    assert find_a_date_dir(tmp_path, "261016") is None


def test_find_a_date_dir_without_raw_images_uses_old_order(tmp_path: Path) -> None:
    (tmp_path / "x261014").mkdir()
    (tmp_path / "261014").mkdir()
    assert find_a_date_dir(tmp_path, "261014") == tmp_path / "261014"
