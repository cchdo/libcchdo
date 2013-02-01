import os
import geojson
import numpy as np
from math import atan2, pi, sin, cos
from libcchdo.plot.etopo import ETOPOBasemap, np, colormap_ushydro
# the import of pyplot into the namepsace must be done after etopo
import matplotlib.pyplot as plt
from matplotlib.offsetbox import (AnnotationBbox, TextArea, DrawingArea,
VPacker, HPacker, AnchoredOffsetbox)

font_style = {
        "line":dict(color="k", size=6),
        "complete":dict(color="0.65", size=5),
        "pending":dict(color="r", style="italic", size=5),
        }

def load_geojson(f):
    return geojson.load(f)

def style_text(s, style):
    return TextArea(s, textprops=style)

def pi2(x):
    if x < 0:
        x += 2 *pi
    return x


def outline(linestring, distance=11):
    '''Returns an image map outline of a line string that is 'distance' pixels
    away

    Linestring is expected to be in image coorantes with the origin being the
    top left corner. Formatted as a xy pairs: [(x,y),...,(x,y)]

    The outliner builds two lists of points on either side of the line string, it
    then reverses one side and concatenates the result. The output is formatted
    for use in html imagemaps.
    '''
    if len(linestring) < 2:
        #TODO add actual error reporting
        print "not a line"

    angles = []
    actual = []
    for i, point1 in enumerate(linestring):
        try:
            point2 = linestring[i + 1]
            dx = point2[0] - point1[0]
            dy = point1[1] - point2[1]
            dt = pi2(atan2(dy, dx)) 
            if len(actual) is not 0:
                dp = actual[i-1]
                # Issues arrise when crossing the positive x axis
                # This brings everything back into something reasonable
                if (dp - dt) > pi:
                    dt += 2 * pi
                if (dp - dt) < -pi:
                    dp += 2 * pi
                dn = pi2((dp + (dt-pi))/2.0 + (pi/2))
                angles.append(dn)
            else:
                angles.append(dt)
            actual.append(dt)
        except IndexError:
            angles.append(dt)

    right = []
    left = []
    for i, point in enumerate(linestring):
        r = pi2(angles[i] - pi/2)
        l = pi2(angles[i] - 3 * pi/2)

        rx = cos(r) * distance
        ry = sin(r) * distance
        lx = cos(l) * distance
        ly = sin(l) * distance

        rp = [int(point[0] - rx), int(point[1] + ry)]
        lp = [int(point[0] - lx), int(point[1] + ly)]
        
        right.append(rp)
        left.append(lp)

    left = left[::-1]
    left = np.array(left).flatten()
    right = np.array(right).flatten()
    outline = np.concatenate((right, left))
    #imagemap needs identical first and last
    outline = np.append(outline, (outline[0], outline[1])) 
    
    return ",".join([str(o) for o in outline])

def box_outline(c_point, num_dates):
    '''Take the center point and the number of dates contained within a label
    box and calcualte a rough (not exact) imagemap outline for use in HTML
    imagemaps 
    
    Center point should be in image (display) coordinates as an xy pair and
    num_dates be an integer
    '''
    cx, cy = c_point
    def t(cx, cy, x, y):
        ul = [cx - x, cy - y]
        ur = [cx - x, cy + y]
        ll = [cx + x, cy - y]
        lr = [cx + x, cy + y]
        return ul, ur, ll, lr

    if num_dates == 2:
        x = 35 
        y = 20
        ul, ur, ll, lr = t(cx, cy, x, y)
    if num_dates == 1:
        x = 15 
        y = 20
        ul, ur, ll, lr = t(cx, cy, x, y)
    if num_dates == 0:
        x = 15 
        y = 15
        ul, ur, ll, lr = t(cx, cy, x, y)
    
    return ",".join([str(int(o)) for o in ul + ur + lr + ll + ul])

def generate_title(d_min, d_max, ax):
    '''This generates the title box artist to be added to the top of the map.
    If the title needs to be changed, this is where to do it.

    The dates d_min and d_max should be years.

    If the title location needs to be adjusted, this is where that happens
    (bbox_to_anchor).
    '''
    title = TextArea("Cruises for the U.S. Global Ocean Carbon and Repeat "
            "Hydrography Program, {0}-{1}".format(d_min, d_max), textprops=dict(size=10))
    subt1 = TextArea("(", textprops=dict(size=8))
    subt2 = TextArea("red italic", textprops=dict(color="r",style="italic",size=8))
    subt3 = TextArea("indicates pending cruise;", textprops=dict(size=8))
    subt4 = TextArea("grey", textprops=dict(color="grey",size=8))
    subt5 = TextArea("indicates completed cruises; black indicates funded cruises)",
            textprops=dict(size=8))
    
    subt = HPacker(children=[subt1,subt2,subt3,subt4,subt5], align="center", pad=0,
            sep=2)
    title = VPacker(children=[title,subt], align="center", pad=0, sep=8)
    t = AnchoredOffsetbox(loc=3, child=title, pad=0, bbox_to_anchor=(-0.02, 1.02),
            bbox_transform=ax.transAxes, borderpad=0, frameon=False)
    return t

lines = []
outlines = []
b_outlines = []
filenames = []
def gen_plots(f, save_dir):

    g = load_geojson(f)
    
    for i in range(len(g["features"]) + 2):
        bm = ETOPOBasemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,\
                        llcrnrlon=20,urcrnrlon=380,lat_ts=20,resolution='c')
        bm.draw_gmt_fancy_border(10)
        bm.draw_etopo(5,5, cmtopo=colormap_ushydro)
    
        fig = plt.gcf()
        ax = bm.axes
        fig.set_dpi(180)
        fig.set_size_inches(1024.0/180, 900.0/180)
        fig.set_tight_layout(True)
    
        dates = []
        for line in g["features"]:
            l = style_text(line["properties"]["title"], font_style["line"])
            years = []
            for s in line["properties"]["completed"]:
                years.append(style_text(s, font_style["complete"]))
                dates.append(s)
            for s in line["properties"]["pending"]:
                years.append(style_text(s, font_style["pending"]))
                dates.append(s)
            y_max = max(dates)
            y_min = min(dates)
        
            box1 = HPacker(children=years, align="center", pad=0, sep=2)
            box = VPacker(children=[l,box1], align="center", pad=0, sep=2)
        
            lon, lat = line["properties"]["box"]
            lonp, latp = bm(lon, lat)
            
            l = np.array(line["geometry"]["coordinates"])
            lons, lats = [x + 360 if x < 20 else x for x in l[:, 0]], l[:, 1]
            xs, ys = bm(lons, lats)
    
            if i < len(g["features"]) and g["features"][i] != line:
                anchored_box = AnnotationBbox( box, (lonp,latp), fontsize=5,
                    bboxprops=dict(alpha=0.5))
                bm.plot(xs,ys,'k-', lw=2, solid_capstyle='round', alpha=0.5)
    
            elif i < len(g["features"]) or i == len(g["features"]):
                anchored_box = AnnotationBbox( box, (lonp,latp), fontsize=5,
                    bboxprops=dict(alpha=1.0))
                bm.plot(xs,ys,'k-', lw=2, solid_capstyle='round', alpha=1.0)
    
            elif i == (len(g["features"]) + 1):
                anchored_box = AnnotationBbox( box, (lonp,latp), fontsize=5,
                    bboxprops=dict(alpha=0.5))
                bm.plot(xs,ys,'k-', lw=2, solid_capstyle='round', alpha=0.5)
        
        
            ax.add_artist(anchored_box)
            if len(g["features"]) > len(outlines):
                # There are magic numbers here, specifically the 1.21 - 124 and the
                # 900. The 900 is the image height that needs to be reversed. The
                # linear equation present was derived emperically, it may need to
                # be changed in the future if the imagemaps seem to have an offset
    
                img_map = ax.transData.transform(zip(xs,ys))
                outlines.append(outline(zip(img_map[:,0] * 1.21 - 124, 900 - img_map[:,1])))
    
                box = ax.transData.transform((lonp, latp))
                b_outlines.append(box_outline((box[0] * 1.21 -124, 900 - box[1]), len(years)))
    
                lines.append(line['properties']['line'])
        
        t = generate_title(y_min, y_max, ax)
    
        ax.add_artist(t)
    
        ft = '.png'
        if i < len(g["features"]):
            filename = "ushydro_black_" + g['features'][i]['properties']['line'].split("/")[0] + ft
        elif i == len(g["features"]):
            filename = "ushydro_black" + ft
        elif i == (len(g["features"]) + 1):
            filename = "ushydro_grey" + ft
    
        filenames.append(filename)
        plt.savefig(os.path.join(save_dir, filename), dpi=180, bbox="tight",
                bbox_extra_artists=[t])
        plt.clf()

def gen_html(base_url):
    print '''<body>'''
    print '''<img name="ushydro_black" src="{0}" width="1024" height="900" border="0"
    id="ushydro_black" usemap="#m_ushydro_black" alt="" />'''.format(
            base_url + str(filenames[-2]))
    
    print '''<map name="m_ushydro_black" id="m_ushydro_black">'''
    for i, outline in enumerate(outlines):
        print '''<area class="l_{1}" shape="poly" coords="{0}"
        href="http://cchdo.ucsd.edu/search?query={1}">'''.format(outline,
                 lines[i])
        print '''<area class="l_{1}" shape="poly" coords="{0}"
        href="http://cchdo.ucsd.edu/search?query={1}">'''.format(b_outlines[i],
                 lines[i])
    print '''</map>'''
    print '''
    <script>
      var suffixes = {0};'''.format([line.encode("utf8") for line in lines])
    print '''
      for (var i = 0; i < suffixes.length; i++) {
        suffixes[i] = 'black_' + suffixes[i];
      }
      suffixes.push('grey');
    
      function preloadImgs(srcs) {
        var img = new Image();
        for (var i = 0; i < srcs.length; i++) {'''
    print '''
          img.src = '{0}ushydro_' + srcs[i] + '.png';'''.format(base_url)
    print '''
        }
      }
      preloadImgs(suffixes);
    
      var image = document.getElementById('ushydro_black');
      var original_src = image.src;
    
      function resetImage() {
        image.src = 'ushydro_grey.png';
      }
    
      image.onmouseover = resetImage;
      image.onmouseout = function(e) {
        if (!e) {
          var e = window.event;
        }
        var relTarg = e.relatedTarget || e.toElement;
        if (relTarg === document.body) {
          image.src = original_src;
        }
      };
    
      var areas = document.getElementsByTagName('AREA');
      for (var i = 0; i < areas.length; i++) {
        var area = areas[i];
        area.onmouseover = (function(area) {
          return function() {
            var classes = area.className.split(' ');
            for (var i = 0; i < classes.length; i++) {
              var cls = classes[i];
              if (cls.substring(0, 2) != 'l_') {
                continue;
              } else {
                var imgsrc = 'ushydro_black_' + cls.substring(2) + '.png';
                image.src = imgsrc;
              }
            }
          };
        })(area);
      }
    </script>'''
    print '''</body>'''

def genfrom_args(args, f):
    gen_plots(f, args.save_dir)
    gen_html(args.html_prefix)

if __name__ == "__main__":
    exit()
