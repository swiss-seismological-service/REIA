import csv
from openquake.hazardlib.shakemap.maps import get_sitecol_shakemap
from openquake.hazardlib.shakemap.gmfs import to_gmfs
from openquake.hazardlib.site import SiteCollection
from openquake.risklib.asset import Exposure

exposure_xml = ['../exposure.xml']

mesh, assets_by_site = Exposure.read(
    exposure_xml, check_dupl=False).get_mesh_assets_by_site()
sitecol = SiteCollection.from_points(
    mesh.lons, mesh.lats)


uridict = {"grid_url": "grid.xml",
           "uncertainty_url": "uncertainty.xml"}

_, shkmp, discarded = get_sitecol_shakemap(
    'usgs_xml', uridict, ['MMI'], sitecol)

gmf_dict = {'kind': 'mmi'}

gmfs = to_gmfs(shkmp, gmf_dict, False, 99, 100, 42, ['MMI'])

# print(gmfs[1].shape)

with open('../sites_gen.csv', 'w') as f:
    writer = csv.writer(f)

    # write the header
    writer.writerow(['site_id', 'lon', 'lat'])

    for site in sitecol:
        # write the data
        if site.id == 18084:
            print(site)
        writer.writerow([site.id,
                         round(site.location.longitude, 5),
                         round(site.location.latitude, 5)])

with open('../gmfs_gen.csv', 'w') as f:
    writer = csv.writer(f)

    # write the header
    writer.writerow(['eid', 'sid', 'gmv_MMI'])

    for i, samples in enumerate(gmfs[1]):
        for j, val in enumerate(samples):
            writer.writerow([j, i, round(val[0], 5)])
