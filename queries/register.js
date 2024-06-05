//require('source-map-support/register')
import App from '@triply/triplydb'

import 'dotenv/config';
import { writeFileSync } from 'fs'
import { readdir, readFile } from 'fs/promises'
import { join, extname, parse } from 'path'

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
          const tableName = parse(filePath).name
          const queryName = `get-${tableName.replace(/[._]/g, '-')}`
          
          console.log(`Registering contents of ${file} as Saved Query '${queryName}'\n`)
          
          let query;
          const params = {
            dataset,
            queryString,
            serviceType: 'virtuoso',
            output: 'response',
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

          result[tableName] = runLink
          console.log(`Available on ${runLink}\n`)
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