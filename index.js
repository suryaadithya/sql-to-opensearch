const fs = require("fs");
const readline = require("readline");
const { Client } = require("@opensearch-project/opensearch");

// OpenSearch configuration
const client = new Client({
  node: "", // Replace with your endpoint
  auth: {
    username: "", // Replace if using basic auth
    password: "",
  },
  ssl: {
    rejectUnauthorized: false, // Set to true in production with proper certificates
  },
});

// Configuration
const BATCH_SIZE = 1000;
const INDEX_NAME = "activitylog";
const FILE_PATH = "activityLog.sql";

const columnNames = [
   // Column names from the ACTIVITYLOG table
];

let bulkBuffer = [];
let totalProcessed = 0;

// Function to safely parse JSON
function safeJSONParse(str) {
  try {
    return JSON.parse(str);
  } catch (e) {
    return str;
  }
}

// Function to clean MySQL string values
function cleanMySQLString(str) {
  if (str === "NULL" || str === null) return null;
  // Remove surrounding quotes if they exist
  if (
    (str.startsWith("'") && str.endsWith("'")) ||
    (str.startsWith('"') && str.endsWith('"'))
  ) {
    str = str.slice(1, -1);
  }
  // Unescape MySQL escaped characters
  return str.replace(/\\'/g, "'").replace(/\\"/g, '"').replace(/\\\\/g, "\\");
}

// Function to extract and parse values from INSERT statement
function parseInsertValues(line) {
  try {
    if (
      !line.trim().toUpperCase().startsWith("INSERT INTO `ACTIVITYLOG` VALUES")
    ) {
      return null;
    }

    // Extract all value groups
    const valuesMatch = line.match(/\((.*?)\)(?=\s*[,)])/g);
    if (!valuesMatch) return null;

    return valuesMatch.map((group) => {
      // Remove outer parentheses
      let content = group.slice(1, -1);
      let values = [];
      let currentValue = "";
      let inQuotes = false;
      let bracketCount = 0;

      // Parse each character to handle nested structures
      for (let i = 0; i < content.length; i++) {
        const char = content[i];

        if (char === "{") bracketCount++;
        if (char === "}") bracketCount--;

        if (char === "'" && content[i - 1] !== "\\") {
          inQuotes = !inQuotes;
        }

        if (char === "," && !inQuotes && bracketCount === 0) {
          values.push(currentValue.trim());
          currentValue = "";
          continue;
        }

        currentValue += char;
      }
      values.push(currentValue.trim());

      // Create record object with proper type conversions
      const record = {};
      values.forEach((val, index) => {
        let processedValue = cleanMySQLString(val);

        switch (columnNames[index]) {
          case "logId":
          case "adminId":
            processedValue = processedValue
              ? parseInt(processedValue, 10)
              : null;
            break;

          case "userAgent":
          case "afterChange":
          case "beforeChange":
            if (processedValue && processedValue !== "") {
              processedValue = safeJSONParse(processedValue);
            } else {
              processedValue = {};
            }
            break;

          case "createdAt":
          case "updatedAt":
            processedValue = processedValue
              ? new Date(processedValue).toISOString()
              : null;
            break;

          case "itemId":
            processedValue = processedValue !== "NULL" ? processedValue : null;
            break;
        }

        record[columnNames[index]] = processedValue;
      });

      return record;
    });
  } catch (error) {
    console.error("Error parsing line:", error);
    return null;
  }
}

// Function to send bulk data to OpenSearch
async function sendBulkToOpenSearch(records) {
  if (records.length === 0) return 0;

  const body = records.flatMap((doc) => [
    { index: { _index: INDEX_NAME } },
    doc,
  ]);

  try {
    const { body: bulkResponse } = await client.bulk({ body });

    if (bulkResponse.errors) {
      const errorItems = bulkResponse.items
        .filter((item) => item.index.error)
        .map((item) => ({
          reason: item.index.error.reason,
          doc_id: item.index._id,
        }));

      if (errorItems.length > 0) {
        console.error(`Bulk operation had ${errorItems.length} errors`);
        console.error("Sample errors:", errorItems.slice(0, 2));
      }
    }

    const successCount = bulkResponse.items.filter(
      (item) => !item.index.error
    ).length;
    return successCount;
  } catch (error) {
    console.error("Bulk insert error:", error);
    return 0;
  }
}

// Main processing function
async function processFile() {
  const startTime = Date.now();
  let lastReportTime = startTime;
  let lineCount = 0;

  const fileStream = fs.createReadStream(FILE_PATH, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  try {
    for await (const line of rl) {
      lineCount++;
      const records = parseInsertValues(line);
      if (!records) continue;

      for (const record of records) {
        bulkBuffer.push(record);

        if (bulkBuffer.length >= BATCH_SIZE) {
          const insertedCount = await sendBulkToOpenSearch(bulkBuffer);
          totalProcessed += insertedCount;

          // Report progress every 30 seconds
          const currentTime = Date.now();
          if (currentTime - lastReportTime > 30000) {
            const duration = (currentTime - startTime) / 1000;
            const rate = Math.round(totalProcessed / duration);
            console.log(`
Progress Report:
- Lines read: ${lineCount.toLocaleString()}
- Records processed: ${totalProcessed.toLocaleString()}
- Time elapsed: ${Math.round(duration)} seconds
- Processing rate: ${rate.toLocaleString()} records/second
`);
            lastReportTime = currentTime;
          }

          bulkBuffer = [];
        }
      }
    }

    // Process remaining records
    if (bulkBuffer.length > 0) {
      const insertedCount = await sendBulkToOpenSearch(bulkBuffer);
      totalProcessed += insertedCount;
    }

    const duration = (Date.now() - startTime) / 1000;
    const rate = Math.round(totalProcessed / duration);

    console.log(`
Import Complete!
- Total lines read: ${lineCount.toLocaleString()}
- Total records processed: ${totalProcessed.toLocaleString()}
- Total time: ${Math.round(duration)} seconds
- Average rate: ${rate.toLocaleString()} records/second
`);
  } catch (error) {
    console.error("Error processing file:", error);
  }
}

// Run the import
processFile().catch((error) => {
  console.error("Import failed:", error);
  process.exit(1);
});
