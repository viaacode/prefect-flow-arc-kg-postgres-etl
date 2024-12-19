import {
	convertAltoXmlFileUrlToSimplifiedJson,
	type SimplifiedAlto,
} from './extract-text-lines-from-alto-internal.js';

convertAltoXmlFileUrlToSimplifiedJson(process.argv[2]).then(
	(simplifiedAltoJson: SimplifiedAlto) => {
		console.log(JSON.stringify(simplifiedAltoJson, null, 2));
	}
);
