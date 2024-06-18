//require('source-map-support/register')
import App from '@triply/triplydb'

import 'dotenv/config';
import { writeFileSync, createWriteStream, existsSync } from 'fs'
import { readdir, readFile, mkdir, rm } from 'fs/promises'
import { join, extname, parse, resolve } from 'path'
import { Readable }  from 'stream'
import { finished }  from 'stream/promises'

async function downloadFile(url, fileName) {
  const res = await fetch(url, {
    headers: {
      Accept: 'text/csv', 
      Authorization: `Bearer ${process.env.TOKEN}`
    }
  });
  if (!existsSync("tables")) 
    await mkdir("tables"); //Optional if you already have downloads directory
  const destination = resolve("./tables", fileName);

  if (existsSync(destination))
    await rm(destination);
  const fileStream = createWriteStream(destination, { flags: 'wx' });
  await finished(Readable.fromWeb(res.body).pipe(fileStream));
}

function getFirstLine(text) {
  var index = text.indexOf("\n");
  if (index === -1) index = undefined;
  return text.substring(0, index);
}

// Directory containing the .sparql files
const directoryPath = './' // change this to your directory path

const triply = App.get({ token: process.env.TOKEN })
async function run() {
  const account = await triply.getAccount(process.env.ACCOUNT)
  const dataset = await account.getDataset(process.env.DATASET)
  const result = {}

  try {
    const files = await readdir(directoryPath)

    for (const file of files) {
      const filePath = join(directoryPath, file)

      // Check if the file has a .sparql extension
      if (extname(file) === '.sparql') {
        try {
          const queryString = await readFile(filePath, 'utf8')

          // Get dependencies from first line
          let position = 0
          const match = getFirstLine(queryString).match(/# position:\s*(\d)/)
          if (match) {
            // Split the captured string by commas and strip any extra whitespace
            position = parseInt(match[1],10);
          }

          const tableName = parse(filePath).name
          const queryName = `get-${tableName.replace(/[._]/g, '-')}`.substring(0,40) // querynames can only be 40 chars long
          
          console.log(`Registering contents of ${file} as Saved Query '${queryName}'\n`)
          
          let query;
          const params = {
            dataset,
            queryString,
            serviceType: 'virtuoso',
            output: 'response',
            variables: [
              {
                name: 'since',
                termType: 'Literal',
                datatype: 'http://www.w3.org/2001/XMLSchema#dateTime'
              }
            ]
          }

          try {
            query = await account.getQuery(queryName)
            await query.delete()
            console.log(`Query ${queryName} deleted.\n`)
          } catch (error) {
            console.log(`Query ${queryName} does not exist.\n`)
          }
          query = await account.addQuery(queryName, params)
          const runLink = await query.getRunLink()

          result[tableName] = { url: runLink, position }
          console.log(`Available on ${runLink}\n`)
          
          if (process.env.FETCH_RESULT) {
            const path = `${tableName}.csv`
            console.log(`Downloading result from ${runLink} to ${path} \n`)
            downloadFile(runLink, path)
          }
          console.log('\n--------------------------------------------\n')
        } catch (readErr) {
          console.error(readErr)
        }
      }
    }
  } catch (dirErr) {
    console.error('Unable to scan directory: ' + dirErr)
  }
  writeFileSync('config.json', JSON.stringify(result))
}
run(directoryPath).catch(e => {
  console.error(e)
  process.exit(1)
})
process.on('uncaughtException', function (e) {
  console.error('Uncaught exception', e)
  process.exit(1)
})
process.on('unhandledRejection', (reason, p) => {
  console.error('Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit(1)
})