const program = require('commander');
const Sentiment = require('sentiment'); // Assuming you have the 'sentiment' library installed
const fs = require('fs');
const csv = require('csv-parser');
const compromise = require('compromise');
const { MongoClient } = require('mongodb');


async function importHeadlines(csvFilePath, db) {
  console.time('Import Time');
  const headlines = [];

  try {
    const stream = fs.createReadStream(csvFilePath).pipe(csv());

    for await (const row of stream) {
      headlines.push({ headline: row.headline_text });
    }

    // Use bulkWrite to insert headlines in batches
    const batchSize = 1000; // Adjust the batch size as needed
    let currentIndex = 0;

    while (currentIndex < headlines.length) {
      const batch = headlines.slice(currentIndex, currentIndex + batchSize);
      currentIndex += batchSize;

      console.log(`Inserting batch of ${batch.length} headlines...`); // Debugging

      const result = await db.collection('headlines').bulkWrite(
        batch.map((headline) => ({
          insertOne: {
            document: headline,
          },
        }))
      );

      console.log(`Batch inserted: ${result.insertedCount} headlines`); // Debugging
    }

    console.log("Import complete.");

    console.timeEnd('Import Time');
    console.log(`Imported ${headlines.length} headlines from CSV.`);
  } catch (error) {
    console.error('Error importing headlines:', error);
  }
}


async function processHeadlines(db) {
  const sentiment = new Sentiment();
  const batchSize = 5000; // Adjust this batch size according to your needs

  try {
    const headlinesCollection = db.collection('headlines');
    let skip = 0;
    let batchNumber = 0;

    while (true) {
      const headlines = await headlinesCollection.find().skip(skip).limit(batchSize).toArray();

     // console.log("hkk");

      if (headlines.length === 0) {
        console.log('No more headlines to process. Terminating...');
        break; // No more headlines to process, so exit the loop
      }

      // Create an array to store bulk update operations
      const bulkOps = [];

      for (let index = 0; index < headlines.length; index++) {
        const headlineObj = headlines[index];
        const headlineId = headlineObj._id;
        const headline = headlineObj.headline;
        const entities = [];
        const entityTypes = [];

        // Tokenize the headline into words using 'compromise'
        const tokens = compromise(headline).out('array');

        // Identify named entities (persons, organizations, locations)
        tokens.forEach((token) => {
          // Here, you can implement more sophisticated logic to classify entities
          // For simplicity, we'll return 'person' for any entity in this example
          entities.push(token);
          entityTypes.push('person'); // Replace with your logic
        });

        // Calculate the sentiment score using 'sentiment' library
        const sentimentAnalysis = sentiment.analyze(headline);
        const sentimentResult =
          sentimentAnalysis.score > 0 ? 'positive' : sentimentAnalysis.score < 0 ? 'negative' : 'neutral';

        // Update the headline object with processed data
        headlineObj.entities = entities;
        headlineObj.entityTypes = entityTypes;
        headlineObj.sentiment = sentimentResult;

        // Push the update operation to the bulkOps array
        bulkOps.push({
          updateOne: {
            filter: { _id: headlineId },
            update: {
              $set: headlineObj,
            },
          },
        });
        console.log(headlineObj);

      }
    

      // Execute the bulk update operation
      const result = await headlinesCollection.bulkWrite(bulkOps);

      if (result.modifiedCount > 0) {
        console.log(`Updated ${result.modifiedCount} headlines.`);
      }

      skip += batchSize;
      batchNumber++;
    }

    console.log('NLP processing complete.');
  } catch (error) {
    console.error('Error processing headlines:', error);
  }
}

program
  .command('import-headlines <path>')
  .description('Import a CSV file of headlines into the system.')
  .action(async (csvFilePath) => {
    const uri = 'mongodb://localhost:27017';
    const client = new MongoClient(uri);

    try {
      await client.connect();
      console.log('Connected to MongoDB');

      const db = client.db();

      await importHeadlines(csvFilePath, db);
    } catch (error) {
      console.error('Error connecting to MongoDB:', error);
    } finally {
      await client.close();
      console.log('Disconnected from MongoDB');
    }
  });

// Define the 'extract-entities' command
program
  .command('extract-entities')
  .description('Process headlines to identify entities and analyze sentiment.')
  .action(async () => {
    const client = new MongoClient('mongodb://localhost:27017');

    try {
      await client.connect();
      console.log('Connected to MongoDB');

      const db = client.db();

      await processHeadlines(db);
      console.log("jskff");
    } catch (error) {
      console.error('Error connecting to MongoDB:', error);
    } finally {
      await client.close();
      console.log('Disconnected from MongoDB');
    }
  });

// Parse the command-line arguments
program.parse(process.argv);
