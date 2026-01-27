const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');

const SOURCE_DIR = 'amazon_economics';
const OUTPUT_FILE = 'temp_ingest/weekly_data.jsonl';

function normalizeColumnName(name) {
    if (!name) return "";
    let clean = name.toString().replace(/[^a-zA-Z0-9]/g, '_');
    clean = clean.replace(/_+/g, '_').replace(/^_|_$/g, '');
    return clean.toLowerCase();
}

function extractDatesFromFilename(filename) {
    const match = filename.match(/(\d{1,2}-\d{1,2}-\d{2}) to (\d{1,2}-\d{1,2}-\d{2})/);
    if (match) {
        return { start: match[1], end: match[2] };
    }
    return { start: null, end: null };
}

function formatDate(dateStr) {
    // Input M-D-YY or MM-DD-YY, Output YYYY-MM-DD
    if (!dateStr) return null;
    const parts = dateStr.split('-');
    const m = parts[0].padStart(2, '0');
    const d = parts[1].padStart(2, '0');
    let y = parts[2];
    if (y.length === 2) y = '20' + y;
    return `${y}-${m}-${d}`;
}

function processFile(filePath) {
    console.log(`Processing ${filePath}...`);
    const filename = path.basename(filePath);
    const dates = extractDatesFromFilename(filename);
    
    if (!dates.start) {
        console.warn(`Skipping ${filename}: No dates found in filename.`);
        return [];
    }

    const workbook = XLSX.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    
    // Convert to JSON with array of arrays to handle headers manually
    const data = XLSX.utils.sheet_to_json(sheet, { header: 1 });
    
    if (data.length < 2) return [];

    const row0 = data[0]; // Category
    const row1 = data[1]; // Metric
    
    const headers = [];
    let lastCat = "info";

    for (let i = 0; i < row0.length; i++) {
        let cat = row0[i];
        let metric = row1[i];
        
        if (cat && !cat.toString().startsWith('Unnamed')) {
            lastCat = cat;
        }
        
        let colName;
        if (!metric || metric.toString().startsWith('Unnamed')) {
            colName = lastCat;
        } else {
            colName = `${lastCat}_${metric}`;
        }
        
        headers.push(normalizeColumnName(colName));
    }

    // Process rows starting from index 2
    const records = [];
    for (let i = 2; i < data.length; i++) {
        const row = data[i];
        // Skip empty rows
        if (!row || row.length === 0) continue;

        const record = {};
        let isEmpty = true;
        
        headers.forEach((header, index) => {
            let val = row[index];
            if (val !== undefined && val !== null && val !== "") {
                isEmpty = false;
            }
            record[header] = val;
        });

        if (!isEmpty) {
            record['report_start_date'] = formatDate(dates.start);
            record['report_end_date'] = formatDate(dates.end);
            record['source_file'] = filename;
            records.push(record);
        }
    }
    
    return records;
}

function main() {
    if (!fs.existsSync('temp_ingest')) {
        fs.mkdirSync('temp_ingest');
    }
    
    const files = fs.readdirSync(SOURCE_DIR).filter(f => f.endsWith('.xlsx'));
    let allData = [];
    
    files.forEach(f => {
        const filePath = path.join(SOURCE_DIR, f);
        try {
            const records = processFile(filePath);
            allData = allData.concat(records);
        } catch (e) {
            console.error(`Error processing ${f}:`, e);
        }
    });
    
    const outStream = fs.createWriteStream(OUTPUT_FILE);
    allData.forEach(entry => {
        outStream.write(JSON.stringify(entry) + '\n');
    });
    outStream.end();
    
    console.log(`Written ${allData.length} rows to ${OUTPUT_FILE}`);
}

main();
