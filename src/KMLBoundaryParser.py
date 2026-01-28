import sys

from lxml import objectify
from xmrgprocessing.boundary.boundariesparse import BoundaryParser
from shapely import to_geojson
from shapely.geometry import Polygon
import geojson
class KMLHUCBoundaryParser(BoundaryParser):
    def _do_parsing(self, **kwargs):
        filename = kwargs.get('filepath', None)
        boundaries_tuples = []
        try:
            kmlFile = open(filename, 'r')
        except Exception as e:
            if (self.logger):
                self.logger.exception(e)
        else:
            try:
                kmlRoot = objectify.parse(kmlFile).getroot()

                for child in kmlRoot.Document.iterchildren():
                    pmCnt = 0
                    for pm in child.Placemark:
                        polypoints = []
                        for simpleData in pm.ExtendedData.SchemaData.iterchildren():
                            if (simpleData.attrib):
                                if 'name' in simpleData.attrib:
                                    if (simpleData.attrib['name'] == "HUC_12"):
                                        watershedName = simpleData.text
                                        break
                        polygon = pm.Polygon.outerBoundaryIs.LinearRing.coordinates
                        points = polygon.text.split(' ')
                        for point in points:
                            parts = point.split(',')
                            polypoints.append((float(parts[0]),float(parts[1])))
                        poly_json = geojson.loads(to_geojson(Polygon(polypoints)))
                        boundaries_tuples.append((watershedName, poly_json))

                        pmCnt += 1
            except Exception as e:
                self._logger.exception(e)
        return boundaries_tuples

if __name__ == '__main__':
    kml_filename = sys.argv[1]
    parser = KMLHUCBoundaryParser(unique_id="1")
    boundaries_tuple = parser.parse(filepath=kml_filename)
    print(boundaries_tuple)
