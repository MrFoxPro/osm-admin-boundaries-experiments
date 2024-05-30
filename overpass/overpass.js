import fs from 'node:fs'

async function d(level) {
	const req = `
		[timeout:300];
		rel[admin_level=${level}][type=boundary][boundary=administrative];
		out geom qt;
	`
	const response = await fetch("https://maps.mail.ru/osm/tools/overpass/api/interpreter", {
		method: "POST",
		body: "data=" + encodeURIComponent(req),
	}).then(x => x.arrayBuffer())
	
	let buf = Buffer.from(response);
	fs.writeFileSync(`al${level}.geom.osm`, buf);
}

await d(2)
await d(4)



// Correct example:
// rel[admin_level=2][type=boundary][boundary=administrative];
// (._;>;);
// out body;



// geojson
// convert item ::=::,::geom=geom(),_osm_type=type();
