import https from 'node:https';
import _ from 'lodash';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { createWriteStream } from 'node:fs';
import * as parser from 'xml2json';
import { MultiProgressBars } from 'multi-progress-bars';
import * as chalk from 'chalk';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import dotenv from 'dotenv'

dotenv.config({ path: '../.env' })
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

const locations = [
    {
        name: 'Gioia del Colle',
        value: '40.7779, 16.9115'
    }
]

/**
 * 
 */
const _getFootPrintName = (location) => {
    let fpn = _.replace(location.name, "'", " ")
    fpn = _.snakeCase(fpn);
    return fpn.toLowerCase();
}

/**
 * 
 * @param {*} product 
 * @returns 
 */
async function makeDirectory(location, product) {
  const projectFolder = join(__dirname, '..', 'datasets', _getFootPrintName(location), product.key);
  const dirCreation = await mkdir(projectFolder, { recursive: true });
  return [dirCreation, projectFolder];
}

/**
 * 
 */
async function processResponse (responseXml) {
    return await new Promise((resolve, reject) => {
        const result = []
        const resultJSon = JSON.parse(parser.toJson(responseXml));
        _.forEach(resultJSon["feed"]["entry"], entry => {

            const date = _.find(entry["date"], d => {
                return d["name"] === "ingestiondate"
            });

            const link = _.find(entry["link"], l => {
                return !l["rel"]
            })

            const item = {
                title: entry.title,
                date: date["$t"],
                link: link["href"]
            };

            result.push(item);
        });

        resolve(_.sortBy(result, ['date', 'title'] ));
    });
}

/**
 * 
 */
async function download(dir, link, name) {

    return await new Promise((resolve, reject) => {

        let dataset = createWriteStream(`${dir}/${name}`);

        const req = https.get(link, options, res => {
            
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
 * @param {*} product 
 * @returns 
 */
async function search(location, product) {
    return await new Promise((resolve, reject) => {

        let url = `https://s5phub.copernicus.eu/dhus/search?q=`
        const footprint = `footprint:"Intersects(${location.value})"`;
        const range = ` AND ${process.env.RANGE}`;
        const productType = ` AND producttype:${product["name"]}`;
        url += `${footprint}${range}${productType}`;
        
        const req = https.get(url, options, res => {
            var chunks = [];
            let len = parseInt(res.headers['content-length'], 10);
            let response = {
                err: null,
                data: null
            };

            res.on("data", (chunk) => {
                chunks.push(chunk);
            });

            res.on("end", (chunk) => {

                response.data = res.statusCode === 200 ?
                                Buffer.concat(chunks).toString('utf8') :
                                null;
                
                response.err = res.statusCode === 200 ?
                                 null :
                                 'Non posso leggere i datasets da Copernicus'

                resolve(response);
            });

            res.on("error", (error) => {
                response.err = error
                reject(response);
            });
        });

        req.end();
        
    });
}

/**
 * 
 */
async function main_downloads(dir, list) {
    // let index_downloads = 0;

    _.forEach(list, l => {
        mpb.addTask(l.title, { type: 'percentage', barColorFn: chalk.yellow });
    });
    

    const promises = _.map(list, async l => {
        return await download(dir, l.link, l.title)
    });

    const files = await Promise.all(promises);
    await mpb.promise;

    /*
    do {
        const file = await download(dir, list[index_downloads].link, list[index_downloads].title)
        index_downloads++;
    } while (index_downloads < _.size(list))
    */
}

/**
 * 
 */
async function main_products(location, pollution) {

    const product = _.find(products, p => {
        return p.key.toUpperCase() === pollution.toUpperCase()
    });

    if (product) {
        let [dir, dirStr] = await makeDirectory(location, product).catch(console.error);
        const resultXml = await search(location, product);
        const listDownloads = await processResponse(resultXml.data);
        await main_downloads(dirStr, listDownloads);
    } else {
        console.error('ERROR: POLLUTION WRONG!');
    }

}

/**
 * 
 */
async function main(location, pollution) {

    const l = _.find(locations, o => {
        return o.name.toUpperCase() === location.toUpperCase()
    })

    if (location) {
        const projectFolder = join(__dirname, '..', 'datasets', _getFootPrintName(location));
        await mkdir(projectFolder, { recursive: true });
        await main_products(l, pollution);
    } else {
        console.error('ERROR: LOCATION WRONG!');
    }
    
    
}

const args = process.argv ? process.argv.slice(2) : []
const location = _.size(args) > 0 ? args[0] : process.env.LOCATION;
const pollution = _.size(args) > 0 ? args[1] : process.env.POLLUTION;

main(location, pollution);