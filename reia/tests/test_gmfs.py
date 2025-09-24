import shutil
from pathlib import Path

import pandas as pd
import pytest

from reia.services.gmfs import GMFService


def test_sample_from_csv(tmp_path, monkeypatch):
    data_dir = Path(__file__).parent / "data" / "gmfs"
    exposure_xml = data_dir / "exposure_test.xml"
    exposure_csv = data_dir / "exposure_test.csv"
    psa03_csv = data_dir / "psa03_m51.csv"
    psa06_csv = data_dir / "psa06_m51.csv"

    tmp_data_dir = tmp_path / "gmfs"
    tmp_data_dir.mkdir()

    tmp_xml = tmp_data_dir / exposure_xml.name
    shutil.copy(exposure_xml, tmp_xml)
    shutil.copy(exposure_csv, tmp_data_dir / exposure_csv.name)

    monkeypatch.setattr(
        GMFService,
        "_calculate_mean_association_distance",
        lambda self, data, factor: 1_000_000,
    )

    monkeypatch.chdir(tmp_path)

    GMFService().sample_from_csv(
        [str(tmp_xml)],
        [str(psa03_csv), str(psa06_csv)],
        num_gmfs=5,
    )

    sites_path = tmp_path / "sites_gen.csv"
    gmfs_path = tmp_path / "gmfs_gen.csv"

    assert sites_path.exists()
    assert gmfs_path.exists()

    sites_df = pd.read_csv(sites_path)
    gmfs_df = pd.read_csv(gmfs_path)

    assert list(sites_df.columns) == ["site_id", "lon", "lat"]
    assert list(gmfs_df.columns) == ["sid", "eid", "gmv_SA(0.3)", "gmv_SA(0.6)"]

    sid0 = gmfs_df[gmfs_df["sid"] == 0].sort_values("eid")
    assert sid0["gmv_SA(0.3)"].tolist() == pytest.approx(
        [0.00765, 0.00452, 0.01470, 0.00447, 0.00574],
        abs=1e-5,
    )
    assert sid0["gmv_SA(0.6)"].tolist() == pytest.approx(
        [0.00171, 0.00329, 0.00379, 0.00365, 0.00345],
        abs=1e-5,
    )

    assert gmfs_df["gmv_SA(0.3)"].mean() == pytest.approx(0.0126388978,
                                                          abs=1e-6)
    assert gmfs_df["gmv_SA(0.6)"].mean() == pytest.approx(0.0046657511,
                                                          abs=1e-6)


def test_sample_from_shakemap(tmp_path, monkeypatch):
    data_dir = Path(__file__).parent / "data" / "gmfs"
    exposure_xml = data_dir / "chile_exposure_model.xml"
    grid_xml = data_dir / "chile_grid.xml"
    uncertainty_xml = data_dir / "chile_uncertainty.xml"

    tmp_data_dir = tmp_path / "shakemap"
    tmp_data_dir.mkdir()

    tmp_exposure = tmp_data_dir / exposure_xml.name
    shutil.copy(exposure_xml, tmp_exposure)
    shutil.copy(data_dir / "chile_exposure_model.csv",
                tmp_data_dir / "chile_exposure_model.csv")
    tmp_grid = tmp_data_dir / grid_xml.name
    shutil.copy(grid_xml, tmp_grid)
    tmp_uncertainty = tmp_data_dir / uncertainty_xml.name
    shutil.copy(uncertainty_xml, tmp_uncertainty)

    monkeypatch.chdir(tmp_path)

    GMFService().sample_from_shakemap(
        [str(tmp_exposure)],
        str(tmp_grid),
        str(tmp_uncertainty),
        ['SA(0.3)', 'SA(1.0)'],
    )

    sites_path = tmp_path / "sites_gen.csv"
    gmfs_path = tmp_path / "gmfs_gen.csv"

    assert sites_path.exists()
    assert gmfs_path.exists()

    sites_df = pd.read_csv(sites_path)
    gmfs_df = pd.read_csv(gmfs_path)

    assert list(sites_df.columns) == ["site_id", "lon", "lat"]
    assert list(gmfs_df.columns) == ["sid", "eid", "gmv_SA(0.3)", "gmv_SA(1.0)"]

    sid0 = gmfs_df[gmfs_df["sid"] == 0].sort_values("eid").head(5)
    assert sid0["gmv_SA(0.3)"].tolist() == pytest.approx(
        [0.02585, 0.08231, 0.04615, 0.03679, 0.01702],
        abs=1e-5,
    )
    assert sid0["gmv_SA(1.0)"].tolist() == pytest.approx(
        [0.02385, 0.01816, 0.01249, 0.03008, 0.02328],
        abs=1e-5,
    )

    assert gmfs_df["gmv_SA(0.3)"].mean() == pytest.approx(0.04983328, abs=1e-6)
    assert gmfs_df["gmv_SA(1.0)"].mean() == pytest.approx(0.02681144, abs=1e-6)


def test_export_from_datastore(tmp_path):
    data_dir = Path(__file__).parent / "data" / "gmfs"
    datastore_src = data_dir / "nepal.hdf5"

    datastore_path = tmp_path / "calc_12345.hdf5"
    shutil.copy(datastore_src, datastore_path)

    gmf_data, site_collection = GMFService().export_from_datastore(
        str(datastore_path))

    assert gmf_data.shape == (8307, 4)
    assert list(gmf_data.columns) == ["sid", "eid", "gmv_PGA", "gmv_SA(0.3)"]
    assert site_collection.shape == (407, 3)
    assert list(site_collection.columns) == ["site_id", "lon", "lat"]

    sample_row = gmf_data[(gmf_data["sid"] == 233) & (gmf_data["eid"] == 8)]
    assert not sample_row.empty
    sample = sample_row.iloc[0]
    assert sample["gmv_PGA"] == pytest.approx(0.005419, abs=1e-6)
    assert sample["gmv_SA(0.3)"] == pytest.approx(0.069012, abs=1e-6)

    assert gmf_data["gmv_PGA"].mean() == pytest.approx(0.063709, abs=1e-6)
    assert gmf_data["gmv_SA(0.3)"].mean() == pytest.approx(0.126406, abs=1e-6)

    first_site = site_collection.iloc[0]
    assert first_site["lon"] == pytest.approx(81.55049, abs=1e-5)
    assert first_site["lat"] == pytest.approx(30.36128, abs=1e-5)
