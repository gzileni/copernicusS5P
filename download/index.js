import https from 'node:https';
import _ from 'lodash';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { createWriteStream } from 'node:fs';
import { MultiProgressBars } from 'multi-progress-bars';
import * as chalk from 'chalk';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import dotenv from 'dotenv';
import axios from 'axios';
import * as turf from '@turf/turf'

dotenv.config({ path: '../.env' });

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const mpb = new MultiProgressBars({
    initMessage: ' Downloaded Datasets ',
    anchor: 'top',
    persist: true,
    border: true,
});

const options = {
    'headers': {
        'Authorization': `Basic czVwZ3Vlc3Q6czVwZ3Vlc3Q=`
    }
};

const products = [
    {
        name: 'L2__SO2___',
        key: "sulfurdioxide",
        value: "Sulfur Dioxide (SO2)"
    },
    {
        name: 'L2__NO2___',
        key: "nitrogendioxide",
        value: "Nitrogen Dioxide (NO2)"
    },
    {
        name: 'L2__HCHO__',
        key: "formaldehyde",
        value: "Formaldehyde (HCHO)"
    },
    {
        name: 'L2__CO____',
        key: "carbonmonoxide",
        value: "Carbon Monoxide (CO)"
    },
    {
        name: 'L2__AER_AI',
        key: "aerosolI",
        value: "UV Aerosol Index"
    },
    {
        name: 'L2__AER_LH',
        key: "aerosolH",
        value: "UV Aerosol Height"
    }
];

/**
 * 
 * @param {*} location 
 * @returns 
 */
const _getFootPrintName = (location) => {
    let fpn = _.replace(location, "'", " ")
    fpn = _.snakeCase(fpn);
    return fpn.toLowerCase();
}

/**
 * 
 * @param {*} product 
 * @returns 
 */
async function makeDirectory(product) {
  const projectFolder = join(__dirname, '..', 'datasets', _getFootPrintName(process.env.LOCATION), product.key);
  await mkdir(projectFolder, { recursive: true });
  return projectFolder;
}

/**
 * 
 * @param {*} dir 
 * @param {*} link 
 * @param {*} name 
 * @returns 
 */
async function download(dir, link, name) {

    return new Promise((resolve, reject) => {

        let dataset = createWriteStream(`${dir}/${name}`);

        https.get(link, options, res => {
            
            let len = parseInt(res.headers['content-length'], 10);
            let cur = 0;

            res.on('data', function (chunk) {
                cur += chunk.length;
                const perc = parseFloat((cur / len));
                mpb.updateTask(name, { percentage: perc });
            });

            res.on('end', chunk => {
                dataset.end();
                mpb.done(name, { message: 'Download finished.' });
                resolve(null);
            });

            res.on("error", (error) => {
                console.log(error);
                reject(error)
            });
            
            res.pipe(dataset);
        });
    });
}

/**
 * 
 * @param {*} dir 
 * @param {*} list 
 */
async function main_downloads(dir, list) {

    _.forEach(list, l => {
        mpb.addTask(l.title, { type: 'percentage', barColorFn: chalk.yellow });
    });

    const promises = _.map(list, l => {
        return download(dir, l.link, l.title)
    });

    await Promise.all(promises);
    await mpb.promise;

}

/**
 * 
 * @param {*} entries 
 * @returns 
 */
const getListDownloads = (entries) => {
    let result = _.map(entries, entry => {
        const link = _.find(entry.link, l => {
            return !l["rel"]
        });
        return {
            title: entry.title,
            link: link["href"]
        }
    });
    return result;
}

/**
 * 
 * @returns 
 */
const _getFootprint = () => {
    const bbox = process.env.BBOX.split(",")
    const poly = turf.bboxPolygon(bbox);
    const coordinates = _.map(poly.geometry.coordinates[0], c => {
        return c.join(' ')
    });
    const coordinates_str = coordinates.join(',');
    const fp = 'footprint:"Intersects(POLYGON((' + coordinates_str + ')))"'
    return fp
}

/**
 * 
 * @param {*} location 
 * @param {*} product 
 * @returns 
 */
const getUrl = (product) => {
    let url = `https://s5phub.copernicus.eu/dhus/search?q=`;
    const footprint = _getFootprint();
    const range = ` AND ${process.env.RANGE}`;
    const productType = ` AND producttype:${product["name"]}`;
    url += `${footprint}${range}${productType}&format=json`;
    return url
}

/**
 * 
 * @param {*} location 
 * @param {*} pollution 
 */
async function main(location, pollution) {

    const product = _.find(products, p => {
        return p.key.toUpperCase() === pollution.toUpperCase()
    });

    mpb.addTask('Searcher', { type: 'indefinite', message: 'Searching ... ' });

    if (process.env.LOCATION) {
        const projectFolder = join(__dirname, '..', 'datasets', _getFootPrintName(process.env.LOCATION));
        await mkdir(projectFolder, { recursive: true });
        
        if (product) {
            let dirStr = await makeDirectory(product).catch(console.error);
            let url = getUrl(product);
            let entries = [];

            try {
                const response = await axios.get(url, { headers: { 'Authorization': `Basic czVwZ3Vlc3Q6czVwZ3Vlc3Q=` }});
                entries = response.data.feed.entry;
                const listLinks = getListDownloads(entries);
                mpb.done('Searcher', { message: 'Search finished.' });
                await main_downloads(dirStr, listLinks); 
            } catch (e) {
                mpb.done('Searcher', { message: 'Search ERROR.' });
                console.error(e)
            };
        } else {
            mpb.done('Searcher', { message: 'Search ERROR.' })
            console.error('ERROR: POLLUTION WRONG!');
        }

    } else {
        mpb.done('Searcher', { message: 'Search ERROR.' })
        console.error('ERROR: LOCATION WRONG!');
    }
    
}

const args = process.argv ? process.argv.slice(2) : []
const location = _.size(args) > 0 ? args[0] : process.env.LOCATION;
const pollution = _.size(args) > 0 ? args[1] : process.env.POLLUTION;

main(location, pollution);