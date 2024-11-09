import sys
#import elasticsearch
from elasticsearch.helpers import scan
from shapely.geometry import shape, mapping, Point, LineString, Polygon, GeometryCollection
from shapely import wkt, affinity
from shapely.ops import unary_union
from osgeo import ogr

import numpy as np
import math
import geojson
import json
from pprint import pprint

class ProcessRoad:
    
    def __init__(self):
        self.lookDeg250ft = 0.0006852754239072771
        self.lookDeg50ft = self.lookDeg250ft / 5.0
        self.geo_state_roadus = 'geo_state_us'
        self.geoimageindices = ''
        self.geo_coverage_index = ''
        self.geo_roadid = 'roadId'
        # Reset Mile values
        self.milesIntersection = 0.0
        self.milesRoad = 0.0
        self.milesMissing = 0.0
        self.coveredMiles = 0.0
        # Geo's
        self.intersectGeo = GeometryCollection()
        self.quadsegs = 8
        self.cap_style = 1  #cap style 1 = round


    ################
    #Functions
    def getintersectingstate_abbr(self, esClient, geom, geomtype):

        sq = {"query":{"geo_shape":{"geometry":{"shape":{"type":geomtype,"coordinates":geom},"relation":"intersects"},"ignore_unmapped":False,"boost":1.0}},"_source":{"includes":[],"excludes":["geometry"]}}  #Faster than others below
        #sq = {"size":5,"query":{"geo_shape":{"geometry":{"shape":{"type":geomtype,"coordinates":geom},"relation":"intersects"},"ignore_unmapped":False,"boost":1.0}},"_source":{"includes":[],"excludes":["geometry"]}}

        
        #sq = \
        #{"query": {"bool": {"must": {"match_all" : {}},"filter" : {"geo_shape" : {"geometry": {"relation" : "intersects","shape" : {"coordinates": geom, "type" : geomtype}},"ignore_unmapped" : False}}}}}

        res= esClient.search(index=self.geo_state_roadus, body=sq)

        states = []
        #May need to change this to not loop but return single value. #No need as some linearids have 2 hitmapsources.
        for hit in res['hits']['hits']:
            states.append(hit['_source']['STUSPS'])
            #return hit['_source']['STUSPS']
        return states[0]

    def getroad_miles(self, geom):
        
        #return np.asarray(('%.17f'%(geom.length / 180 * math.pi * 3961.0)), dtype=np.float64, order='C')    ##This returns string may still need to give precise decimal places.
        #return np.fromstring(('%.17f'%(geom.length / 180 * math.pi * 3961.0)), dtype=np.float)

        return float('%.17f'%(geom.length / 180 * math.pi * 3961.0))

    def getIntersectingPoint(self, imagesES, rdgeo, imageindices):

        eventLocs = []
        gp = []
        #mlss= ((0, 0),)

        #buffer road to output polygon
        poly_rdbuffered = self.buffer_road(rdgeo, self.lookDeg50ft)

        #if rdgeo.is_ring:
            #ring_convex = self.buffer_road(self.geo_intersection(rdgeo, rdgeo.convex_hull), self.lookDeg50ft)
            #poly_rdbuffered = self.buffer_road(rdgeo, self.lookDeg50ft)
        
        #Using to ensure that holes in polygon are eliminated especially for road roads, so that holes do not pick events.
        outRing = ogr.Geometry(ogr.wkbLinearRing)
        for x, y in poly_rdbuffered.exterior.coords:
            outRing.AddPoint(x, y)

        innerRing = ogr.Geometry(ogr.wkbLinearRing)
        for interior in poly_rdbuffered.interiors:
            for x, y in interior.coords:
                innerRing.AddPoint(x,y)

        poly = ogr.Geometry(ogr.wkbPolygon)
        #print(poly)
        poly.AddGeometry(outRing)
        poly.AddGeometry(innerRing)

        geojson = poly.ExportToJson()
        p_coords = json.loads(geojson)

        for p_coord in p_coords['coordinates']:
            for pt in p_coord:
                gp.append([pt[0], pt[1]])


            '''
            if not ring_convex.is_empty:
                mlss = mapping(ring_convex).get('coordinates')
            
            #gp.extend(self.tuple_to_list(mlss)[0])  #Check length later and if behaviour is different.

            for mls in mlss:
                for ml in mls:
                    gp.append([ml[0], ml[1]])
            '''
        '''     
        else:
            #rdbuffered = self.buffer_road(rdgeo, self.lookDeg50ft)
            gp = np.asarray(poly_rdbuffered.exterior.coords).tolist()
        '''
        #print(gp)
        esq = \
        {
            "size":10000,
            "query": {"geo_polygon":
                {"location":
                    {"points": gp}
                    ,"validation_method":"STRICT","ignore_unmapped":False,"boost":1.0}
                }
            ,"_source":{"includes":["gps_latitude_deg","gps_longitude_deg","gps_bearing_deg","captured_datetime_utc"],"excludes":["location"]}
        }

        imgcount = imagesES.search(index=imageindices, body = esq, size=0)['hits']['total']
    
        eres = scan(client=imagesES, query=esq, size=10000, scroll='10m', index=imageindices, clear_scroll=True, raise_on_error=False) #Setting scroll to 10 mins.
        #eres = scan(client=imagesES, query=esq, size=10000, scroll='1h', index=imageindices, clear_scroll=True, preserve_order=True)
    
        for hit in eres:
            eventLocs.append(hit["_source"])
        
        #print(imgcount)
        return imgcount, eventLocs

    def tuple_to_list(self, tup):
        return list(map(self.tuple_to_list, tup)) if isinstance(tup, (list, tuple)) else tup

    def rotateAroundPoint(self, viewPathLine, eventPoint, radianBearing):
        #loc = ''
        loc = GeometryCollection()

        #if eventPoint is None:
        if eventPoint.is_empty:
            loc = (0,0)
        else:
            loc = eventPoint.coords[0]
    
        rline = affinity.rotate(viewPathLine, radianBearing, (loc[0], loc[1]), use_radians=True)
    
        return rline

    def getViewPortFromImageLocation(self, eventLoc):
        lat = eventLoc.get("gps_latitude_deg")
        long = eventLoc.get("gps_longitude_deg")
        gpsBearing = eventLoc.get("gps_bearing_deg")
        radianBearing= -(gpsBearing * math.pi / 180.0)
    
        eventpoint = Point(long, lat)
   
        #Create a line from the point going north
        viewPathLine = LineString([(eventpoint.x, eventpoint.y), (eventpoint.x, eventpoint.y+self.lookDeg250ft)])

        #Rotate the line towards the bearing
        viewPathLine = self.rotateAroundPoint(viewPathLine, eventpoint, radianBearing)
        
        #Create a buffer area for the view distance        
        eventpoint = self.buffer_road(viewPathLine, self.lookDeg250ft)
        #eventpoint = viewPathLine.buffer(self.lookDeg250ft, quadsegs=8)

        return eventpoint

    #Below not used but is a good reference.
    def get_geom_length(self, geom):
        le = 0.0

        x0, y0, x1, y1 = 0,0,0,0
        for pts in (list(geom.coords)):
            x1 = pts[0]
            y1 = pts[1]
    
            if x0 != 0 and y0 != 0:
                dx = x1 - x0
                dy = y1 -y0
        
                le += math.sqrt(dx * dx + dy * dy)
            
            else:
            
                x0 = x1
                y0 = y1
                continue

            x0 = x1
            y0 = y1
        return le

    def geo_intersection(self, geom1, geom2):
        return geom1.intersection(geom2)

    def geo_difference(self, geom1, geom2):
        return geom1.difference(geom2)

    def getMilesFromLineString(self, geom):
        return (geom.length / 180 * math.pi * 3961.0)

    #Below not used but is a good reference.
    def getMilesFromLineString2(self, geom):
        #print(geom.type)
        if geom.type == 'LineString':
            return (self.get_geom_length(geom)/ 180 * math.pi * 3961.0)
        elif geom.type == 'MultiLineString':        
            dis = 0.0
            #print(dis)
            for g in geom:            
                dis += self.get_geom_length(g)
                #print(dis, ' - ', g)
            return (dis/ 180 * math.pi * 3961.0)
    
    def coveredmiles(self, mile1, mile2):
        return (mile1 + mile2)

    def errormiles(self, mile1, mile2):
        return (mile1 - mile2)

    def buffer_road(self, rdgeo, lookDeg):
         return rdgeo.buffer(lookDeg, quadsegs=self.quadsegs, cap_style=self.cap_style)

    def roadHasBeenProcessed(self, factSource, linear_id):
        return len(factSource.search(self.geo_coverage_index, self.geo_roadid, linear_id)) > 0

    def pointspaceunion(self, eventLocs):
        eventpoints = []

        for eventLoc in eventLocs:
            eventPoint = self.getViewPortFromImageLocation(eventLoc)
            eventpoints.append(eventPoint)

                #if pointSpace.is_empty:
                #    pointSpace = eventPoint
                #else:
                #    pointSpace = pointSpace.union(eventPoint)
        return unary_union(eventpoints)
    ################

    def startprocessroad(self, rec_count, esClient, imgClient, factSource, linear_id, hit):

        roadName = ''
        RTTYP = ''
        LINEARID = ''
        MTFCC = ''        
        coord = ''
        coordtype = ''
        roadgeo = ''

        #print to log
        cmsg = ''

        try:

            roadName = hit.get('FULLNAME') or '(Unknown Name)'

            RTTYP = hit.get('RTTYP')
            LINEARID = hit.get('LINEARID')
            MTFCC = hit.get('MTFCC')
            coord = hit.get('geometry').get('coordinates')

            roadgeo = shape(hit.get('geometry'))
            coordtype = hit.get("geometry").get("type")

            pprint(f'Working on ({linear_id}) {roadName}')

            #Skip the road if it was already been processed
            if self.roadHasBeenProcessed(factSource, linear_id):
                cmsg += f'\t*** Road ({linear_id}) already processed'

                return rec_count, cmsg

            rec_count += 1
        
            state = self.getintersectingstate_abbr(esClient, coord, coordtype)

            cmsg += f"<<<< Info for: {LINEARID}; road name: {roadName} ---------->>>>"
            cmsg += f"\nSate: {state}"

            self.milesRoad= self.getMilesFromLineString(roadgeo)

            #if roadgeo.is_ring:

            #roadBuffered = roadgeo.buffer(self.lookDeg50ft, quadsegs=8, cap_style=1) #cap style 1 = round
            lineseg = len(roadgeo.coords)

            cmsg += f"\nHas {lineseg} line segments"
            cmsg += f"\nRoad miles: {self.milesRoad}"

            events = self.getIntersectingPoint(imgClient, roadgeo, self.geoimageindices)
            #events = self.getIntersectingPoint(imgClient, roadBuffered, self.geoimageindices)
            
            imageCount = events[0]
            eventLocs = events[1]

            cmsg += f"\nEvents: {imageCount}"
            cmsg += f"\nFound {imageCount} matches"

            #print('Events:', imageCount)
            #print(f'Found {imageCount} matches')

            pointSpace = GeometryCollection()
            pointSpace = self.pointspaceunion(eventLocs)

            if not pointSpace.is_empty:

                self.intersectGeo = self.geo_intersection(roadgeo, pointSpace)

                self.milesIntersection = self.getMilesFromLineString(self.intersectGeo)
                
                cmsg += f"\nLength of Intersect is {self.milesIntersection}"

                missingGeo = self.geo_difference(roadgeo, self.intersectGeo)
                
                self.milesMissing = self.getMilesFromLineString(missingGeo)
                
                cmsg += f"\nLength of missing is {self.milesMissing}"

                coveredMiles = self.coveredmiles(self.milesMissing, self.milesIntersection)
                
                error = self.errormiles(coveredMiles, self.milesRoad)
                
                cmsg += f"\nTotal Computed is {coveredMiles}, error is {error}"

            map = {}

            map['roadId'] = LINEARID
            map['RoadName'] = roadName
            map['MTFCC'] = MTFCC
            map['RTTYP'] = RTTYP
            map['RoadMiles'] = self.milesRoad
            map['CoveredMiles'] = self.milesIntersection
            map['MissingMiles'] = self.milesMissing
            map['ImageCount'] = imageCount
            #map['images'] = ''#eventLocs    Removing images since it is not necessary.
            map['State'] = state
            map['isRoadRing'] = roadgeo.is_ring

            '''
            if not self.intersectGeo.is_empty:
                map['Coverage'] = None #self.intersectGeo.wkt   Removing 

            if not pointSpace.is_empty:
                map['PointSpace'] = None #pointSpace.wkt        Removing

            if not roadgeo.is_empty:
                map['RoadGeo'] = None #roadgeo.wkt              Removing
            '''

            rmap = geojson.dumps(map)

            res = esClient.index(index = self.geo_coverage_index,  doc_type='_doc', body = rmap, ignore=400)

            #print(" response: '%s'" % (res))

            cmsg += "\n<<<<--------------------------------------------------------->>>>\n"

            return rec_count, cmsg
        
        except:
            error = sys.exc_info()
            pprint(f"nError:  {error}")
            #print('Error: ', sys.exc_info())
