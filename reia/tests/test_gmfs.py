import shutil
from pathlib import Path

import pandas as pd

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
        "_isin_switzerland",
        lambda self,
        data: (
            True,
            0))

    monkeypatch.chdir(tmp_path)

    GMFService().sample_from_csv(
        [str(tmp_xml)],
        str(psa03_csv),
        str(psa06_csv),
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

    assert set(gmfs_df["eid"].unique()) == set(range(5))
    assert not gmfs_df.empty
    assert gmfs_df[["gmv_SA(0.3)", "gmv_SA(0.6)"]].ge(0).all().all()


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
    )

    sites_path = tmp_path / "sites_gen.csv"
    gmfs_path = tmp_path / "gmfs_gen.csv"

    assert sites_path.exists()
    assert gmfs_path.exists()

    sites_df = pd.read_csv(sites_path)
    gmfs_df = pd.read_csv(gmfs_path)

    assert list(sites_df.columns) == ["site_id", "lon", "lat"]
    assert list(gmfs_df.columns) == ["sid", "eid", "SA(0.3)", "SA(1.0)"]

    assert not gmfs_df.empty
    assert gmfs_df[["SA(0.3)", "SA(1.0)"]].ge(0).all().all()
