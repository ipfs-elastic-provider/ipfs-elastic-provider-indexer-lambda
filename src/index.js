'use strict'

const {
  UnixFS: { unmarshal: decodeUnixFs }
} = require('ipfs-unixfs')

const { codecs, blocksTable, primaryKeys, carsTable } = require('./config')
const { logger, elapsed } = require('./logging')
const { openS3Stream } = require('./source')
const { readDynamoItem, writeDynamoItem } = require('./storage-dynamo')
const { forEach } = require('hwp')

function decodeBlock(block) {
  const codec = codecs[block.cid.code]

  if (!codec) {
    logger.error(`Unsupported codec ${block.cid.code} in the block at offset ${block.blockOffset}`)
    throw new Error(`Unsupported codec ${block.cid.code} in the block at offset ${block.blockOffset}`)
  }

  // Decoded the block data
  const data = codec.decode(block.data)

  if (codec.label.startsWith('dag')) {
    const { type, blocks } = decodeUnixFs(data.Data)

    data.Data = { type, blocks }
  }

  return { codec: codec.label, data }
}

async function storeNewBlock(car, type, block, data = {}) {
  for (const link of data?.Links ?? []) {
    link.Hash = link.Hash.toString()
  }

  const cid = block.cid.toString()

  return writeDynamoItem(true, blocksTable, primaryKeys.blocks, cid, {
    type,
    createdAt: new Date().toISOString(),
    cars: [{ car, offset: block.blockOffset, length: block.blockLength }],
    data
  })

  // TODO: Publish to SQS
}

async function appendCarToBlock(block, cars, carId) {
  cars.push({ car: carId, offset: block.blockOffset, length: block.blockLength })

  return writeDynamoItem(false, blocksTable, primaryKeys.blocks, block.cid.toString(), {
    cars: cars.sort((a, b) => {
      return a.offset !== b.offset ? a.offset - b.offset : a.car.localeCompare(b.car)
    })
  })
}

async function main(event) {
  const start = process.hrtime.bigint()

  // For each Record in the event
  let currentCar = 0
  const totalCars = event.Records.length

  for (const record of event.Records) {
    const partialStart = process.hrtime.bigint()

    const car = new URL(`s3://${record.s3.bucket.name}/${record.s3.object.key}`)
    const carId = car.toString().replace('s3://', '')

    currentCar++

    // Check if the CAR exists and it has been already analyzed
    const existingCar = await readDynamoItem(carsTable, primaryKeys.cars, carId)

    if (existingCar?.completed) {
      logger.debug(
        { elapsed: elapsed(start), progress: { records: { current: currentCar, total: totalCars } } },
        `Skipping CAR ${car} (${currentCar} of ${totalCars}), as it has already been analyzed.`
      )

      continue
    }

    // Show event progress
    logger.debug(
      { elapsed: elapsed(start), progress: { records: { current: currentCar, total: totalCars } } },
      `Analyzing CAR ${currentCar} of ${totalCars}: ${car}`
    )

    // Load the file from input
    const indexer = await openS3Stream(car)

    // Store the initial information of the CAR
    await writeDynamoItem(true, carsTable, primaryKeys.cars, carId, {
      bucket: record.s3.bucket.name,
      key: record.s3.object.key,
      createdAt: new Date().toISOString(),
      roots: new Set(indexer.roots.map(r => r.toString())),
      version: indexer.version,
      fileSize: indexer.length,
      currentPosition: indexer.length,
      completed: false
    })

    // For each block in the indexer (which holds the start and end block)
    await forEach(indexer, async function (block) {
      // Show CAR progress
      logger.debug(
        {
          elapsed: elapsed(start),
          progress: {
            records: { current: currentCar, total: totalCars },
            car: { position: indexer.position, length: indexer.length }
          }
        },
        `Analyzing CID ${block.cid}`
      )

      // If the block is already in the storage, fetch it and then just update CAR informations
      const existingBlock = await readDynamoItem(blocksTable, primaryKeys.blocks, block.cid.toString())
      if (existingBlock) {
        await appendCarToBlock(block, existingBlock.cars, carId)
        return
      }

      // Store the block according to the contents
      if (!block.data) {
        await storeNewBlock(carId, 'raw', block)
      } else {
        const { codec, data } = decodeBlock(block)
        await storeNewBlock(carId, codec, block, data)
      }
    })

    // Mark the CAR as completed
    await writeDynamoItem(false, carsTable, 'path', carId, {
      currentPosition: indexer.length,
      completed: true,
      durationTime: elapsed(partialStart)
    })
  }
}

exports.handler = main
