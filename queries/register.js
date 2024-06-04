//require('source-map-support/register')
import App from '@triply/triplydb'

import 'dotenv/config';
import { readdir, readFile } from 'fs/promises'
import { join, extname, parse } from 'path'

// Directory containing the .sparql files
const directoryPath = './' // change this to your directory path

const triply = App.get({ token: process.env.TOKEN })
async function run() {
  const account = await triply.getAccount(process.env.ACCOUNT)
  const dataset = await account.getDataset(process.env.DATASET)

  try {
    const files = await readdir(directoryPath)

    for (const file of files) {
      const filePath = join(directoryPath, file)

      // Check if the file has a .sparql extension
      if (extname(file) === '.sparql') {
        try {
          const queryString = await readFile(filePath, 'utf8')
          const queryName = `get-${parse(filePath).name}`
          console.log(`Adding contents of ${file} as ${queryName}\n`)
          
          const query = await account.addQuery(queryName, {
            dataset,
            queryString,
            serviceType: 'virtuoso',
            output: 'response',
          })
          console.log(`Available on ${await query.getRunLink()}\n`)
          console.log('\n--------------------------------------------\n')
        } catch (readErr) {
          console.error(readErr)
        }
      }
    }
  } catch (dirErr) {
    console.error('Unable to scan directory: ' + dirErr)
  }

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