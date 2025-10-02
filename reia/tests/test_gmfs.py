import configparser
import shutil
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from reia.services.gmfs import GMFConfigurationService


def read_to_buf(file_path: Path) -> StringIO:
    buf = StringIO()
    with open(file_path, 'r') as f:
        buf.write(f.read())
    buf.seek(0)
    buf.name = file_path.name
    return buf


def test_sample_from_custom_csv():
    data_dir = Path(__file__).parent / "data" / "gmfs"
    exposure_xml = data_dir / "ch_exposure.xml"
    exposure_csv = data_dir / "ch_exposure.csv"
    job_file = data_dir / "ch_job.ini"

    psa03_path = data_dir / "psa03_m51.csv"
    psa06_path = data_dir / "psa06_m51.csv"

    exposure_xml_buf = read_to_buf(exposure_xml)
    exposure_csv_buf = read_to_buf(exposure_csv)

    job = configparser.ConfigParser(interpolation=None)
    job.read(job_file)
    job['hazard_source']['files'] = f"{psa03_path},{psa06_path}"

    gmf_config = GMFConfigurationService(job)
    gmfs_buf, sites_buf = gmf_config.get_gmfs(exposure_xml_buf,
                                              exposure_csv_buf)

    sites_df = pd.read_csv(sites_buf)
    gmfs_df = pd.read_csv(gmfs_buf)

    assert list(sites_df.columns) == ["site_id", "lon", "lat"]
    assert list(gmfs_df.columns) == ["sid", "eid", "gmv_SA(0.3)", "gmv_SA(0.6)"]

    sid0 = gmfs_df[gmfs_df["sid"] == 0].sort_values("eid")

    assert sid0["gmv_SA(0.3)"].tolist() == pytest.approx(
        [0.01535, 0.00907, 0.02947, 0.00897, 0.01152],
        abs=1e-5,
    )
    assert sid0["gmv_SA(0.6)"].tolist() == pytest.approx(
        [0.00171, 0.00327, 0.00377, 0.00363, 0.00343],
        abs=1e-5,
    )

    assert gmfs_df["gmv_SA(0.3)"].mean() == pytest.approx(0.0253429066666,
                                                          abs=1e-6)
    assert gmfs_df["gmv_SA(0.6)"].mean() == pytest.approx(0.0046409422222,
                                                          abs=1e-6)


def test_sample_from_shakemap():
    data_dir = Path(__file__).parent / "data" / "gmfs"
    exposure_xml = data_dir / "chile_exposure_model.xml"
    exposure_csv = data_dir / "chile_exposure_model.csv"
    grid_xml = data_dir / "chile_grid.xml"
    uncertainty_xml = data_dir / "chile_uncertainty.xml"

    exposure_xml_buf = read_to_buf(exposure_xml)
    exposure_csv_buf = read_to_buf(exposure_csv)

    job = configparser.ConfigParser(interpolation=None)
    job_file = data_dir / 'chile_job.ini'
    job.read(job_file)

    job['hazard_source']['grid_xml'] = str(grid_xml)
    job['hazard_source']['uncertainty_xml'] = str(uncertainty_xml)

    gmf_config = GMFConfigurationService(job)
    gmfs_buf, sites_buf = gmf_config.get_gmfs(exposure_xml_buf,
                                              exposure_csv_buf)

    sites_df = pd.read_csv(sites_buf)
    gmfs_df = pd.read_csv(gmfs_buf)

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


def test_gmfs_from_datastore(tmp_path):
    data_dir = Path(__file__).parent / "data" / "gmfs"
    datastore_src = data_dir / "nepal.hdf5"
    job_file = data_dir / "nepal_job.ini"
    datastore_path = tmp_path / "calc_12345.hdf5"
    shutil.copy(datastore_src, datastore_path)

    job = configparser.ConfigParser(interpolation=None)
    job.read(job_file)
    job['hazard_source']['dstore_file'] = str(datastore_path)

    gmf_config = GMFConfigurationService(job)
    gmfs_buf, sites_buf = gmf_config.get_gmfs(MagicMock(), MagicMock())

    sites_df = pd.read_csv(sites_buf)
    gmfs_df = pd.read_csv(gmfs_buf)

    assert gmfs_df.shape == (8307, 4)
    assert list(gmfs_df.columns) == ["sid", "eid", "gmv_PGA", "gmv_SA(0.3)"]
    assert sites_df.shape == (407, 3)
    assert list(sites_df.columns) == ["site_id", "lon", "lat"]

    sample_row = gmfs_df[(gmfs_df["sid"] == 233) & (gmfs_df["eid"] == 8)]
    assert not sample_row.empty
    sample = sample_row.iloc[0]
    assert sample["gmv_PGA"] == pytest.approx(0.005419, abs=1e-6)
    assert sample["gmv_SA(0.3)"] == pytest.approx(0.069012, abs=1e-6)

    assert gmfs_df["gmv_PGA"].mean() == pytest.approx(0.063709, abs=1e-6)
    assert gmfs_df["gmv_SA(0.3)"].mean() == pytest.approx(0.126406, abs=1e-6)

    first_site = sites_df.iloc[0]
    assert first_site["lon"] == pytest.approx(81.55049, abs=1e-5)
    assert first_site["lat"] == pytest.approx(30.36128, abs=1e-5)


def test_gmfs_from_csv():
    data_dir = Path(__file__).parent / "data" / "gmfs"
    job_file = data_dir / "ch_basic_job.ini"

    gmfs_path = data_dir.parent / "ria_test" / "gmfs_test.csv"
    sites_path = data_dir.parent / "ria_test" / "sites.csv"

    job = configparser.ConfigParser(interpolation=None)
    job.read(job_file)
    job['hazard']['gmfs_csv'] = str(gmfs_path)
    job['hazard']['sites_csv'] = str(sites_path)

    gmf_config = GMFConfigurationService(job)
    gmfs_buf, sites_buf = gmf_config.get_gmfs(MagicMock(),
                                              MagicMock())

    sites_df = pd.read_csv(sites_buf)
    gmfs_df = pd.read_csv(gmfs_buf)

    assert list(sites_df.columns) == ["site_id", "lon", "lat"]
    assert list(gmfs_df.columns) == ["sid", "eid", "gmv_MMI", "gmv_1"]

    sid0 = gmfs_df[gmfs_df["sid"] == 36807].sort_values("eid")[0:5]

    assert sid0["gmv_MMI"].tolist() == pytest.approx(
        [0.05403812, 0.064683095, 0.0662216, 0.043471977, 0.039713044],
        abs=1e-5,
    )
    assert sid0["gmv_1"].tolist() == pytest.approx(
        [0.07645915, 0.08388496, 0.082209446, 0.06126771, 0.05852616],
        abs=1e-5,
    )

    assert gmfs_df["gmv_MMI"].mean() == pytest.approx(0.303477470988,
                                                      abs=1e-6)
    assert gmfs_df["gmv_1"].mean() == pytest.approx(0.165232480005,
                                                    abs=1e-6)
