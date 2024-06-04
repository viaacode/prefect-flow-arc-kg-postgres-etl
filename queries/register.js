require('source-map-support/register')
import App from '@triply/triplydb'


import { readdir, readFile } from 'fs/promises'
import { join, extname, basename } from 'path'

// Directory containing the .sparql files
const directoryPath = './' // change this to your directory path

const triply = App.get({ token: process.env.TOKEN })
async function run() {
  const user = await triply.getUser()
  const myDataset = await user.getDataset(process.env.DATASET)

  try {
    const files = await readdir(dirPath)

    for (const file of files) {
      const filePath = join(dirPath, file)

      // Check if the file has a .sparql extension
      if (extname(file) === '.sparql') {
        try {
          const queryString = await readFile(filePath, 'utf8')
          const queryName = basename(queryString)
          console.log(`Adding contents of ${file} as ${queryName}\n`)
          
          const query = await user.addQuery(file, {
            dataset: myDataset,
            queryString,

            output: 'response',
          })
          console.log(`Available on ${query.getRunLink()}\n`)
          console.log('\n--------------------------------------------\n')
        } catch (readErr) {
          console.error('Unable to read file: ' + readErr)
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