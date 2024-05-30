osmfilter ~/russia.o5m \
	--drop-version --drop-author \
	--keep="(place=city or place=town or place=village or place=state or place=country or place=region or place=state) and population>100 or boundary=administrative" \
	--drop-ways --drop-author --ignore-dependencies \
	-o=russia_procesed.osm


# osmosis --read-pbf ~/misc/downloads/planet-240520.osm.pbf \
#   --tag-filter accept-nodes place=city \
#   --tag-filter accept-nodes place=town \
#   --tag-filter accept-nodes place=village \
#   --tag-filter reject-ways \
#   --tag-filter reject-relations \
#   --remove-tags keys='*' \
#   --log-progress \
#   --write-pbf ~/misc/planet_filtered.osm.pbf