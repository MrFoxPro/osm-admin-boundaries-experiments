### semi-manual planet parsing
Download full OSM planet in PBF format
Filter it with `cargo run --release -- --filter` (configure as you want) or with `osmfilter`, but need to convert from pbf to osm/o5m, process, and convert back (too much space)
Collect all info you need from filtered file, it's quite fast. Use https://github.com/AndGem/osm_extract_polygon. Works not very well, but code very simple and I just fixed it and recompiled. Maybe will contribue/fork this.
It's better to combine resulting regions in single file, but I didn't find convinient way to deal with this huge files. JavaScript just can't handle this large files.


### overpass 
kinda cringle
slow, unintuitive. doesn't produce PBF. produces OSM in `out geom` mode which is not compatable with most readers (it includes node coordinates directly into node tags, not separate entities)
three ways to get geojson from overpass:
1) download from ui:  https://maps.mail.ru/osm/tools/overpass/index.html (will hang for big queries, e.g. admin_level=4)
2) github.com/tyrasd/osmtogeojson - javascript (oof), slow, need 13gb (lol) to process 800mb file
3) use `convert item ::=::,::geom=geom(),_osm_type=type();` in query. But it produces something weird. 
Results from all ways are different, so probably it's not worth to use overpass at all.
Also, using overpass boundaries results in worse output than using boundaries as in `semi-manual planet parsing`
