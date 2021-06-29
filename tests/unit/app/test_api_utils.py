from app.blueprints.api.utils import ini_to_dict, read_asset_csv, sites_from_assets


def test_read_asset_csv():
    with open('tests/data/exposure_assets.csv') as csv:
        df = read_asset_csv(csv)

    result = [5.966213835966539, 46.15234227634242, 'M3_L',
              32.0, 24607529.576893, 8612635, 91.61953186558736,
              'GE', '6611', '1284']

    assert df.loc[1].values.tolist() == result

    columns = ['taxonomy_concept', 'buildingcount', 'contentvalue_value',
               'occupancydaytime_value', 'structuralvalue_value',
               '_municipality_oid', '_postalcode_oid']

    assert all(col in list(df.columns) for col in columns)


def test_sites_from_assets():
    with open('tests/data/exposure_assets.csv') as csv:
        df = read_asset_csv(csv)
        df['_assetcollection_oid'] = 1

    all_sites, groups = sites_from_assets(df)

    assert float(df.iloc[[8]]['lon']) == all_sites[groups[8]].longitude_value
    assert float(df.iloc[[3]]['lat']) == all_sites[groups[3]].latitude_value
    assert len(all_sites) == 7


def test_ini_to_dict():
    with open('tests/data/risk.ini', 'rb') as file:
        new_dict = ini_to_dict(file)
    assert isinstance(new_dict['shakemap_uri'], dict)
    assert 'calculation_mode' in new_dict
    assert new_dict['number_of_ground_motion_fields'] == 500
