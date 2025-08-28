const fetchInitialDataForDetection = async (url) => {
  try {
    const response = await fetch(`/fetchFile?url=${encodeURIComponent(url)}&num_bytes=4096`);
    if (!response.ok) throw new Error('Network response was not ok.');
    const data = await response.arrayBuffer(); // or response.text() based on your server response type
    return data;
  } catch (error) {
    console.error('Error fetching initial data:', error);
  }
};

const detectFormatFromURI = (uri) => {
    let format;
    const compressionFormats = ['gz', 'bz2']; // List of common compression formats
    const parts = uri.split('.');
    // Check if the URI has a known compression format
    const hasCompression = parts.length > 1 && compressionFormats.includes(parts[parts.length - 1].toLowerCase());

    // Extract the file format extension
    const fileExtension = hasCompression ? parts[parts.length - 2].toLowerCase() : parts[parts.length - 1].toLowerCase();

    switch (fileExtension) {
      case 'geojson': format = 'geojson'; break;
      case 'shp': format = 'shapefile'; break;
      case 'csv': format = 'csv'; break;
      case 'tsv': format = 'csv'; break;
      case 'wkt': format = 'csv'; break;
      case 'gpx': format = 'gpx'; break;
      case 'json': format = 'json+wkt'; break;
      case 'parquet': format = 'geoparquet'; break;
      // Include known compressed formats associated with specific file types
      case 'zip': format = 'shapefile'; break;
      // Add more cases as necessary
      default: format = undefined; // Default or unknown format
    }
    return format;
  };


const detectCSVFormat = (initialData) => {
  // Convert the input data to text
  const text = typeof initialData === "string"? initialData : new TextDecoder("utf-8").decode(initialData);

  const separators = [',', '\t', ' ', ';', ':', '|'];
  const quoteChars = ["", '"', "'"];
  const headerOptions = [false, true];

  let bestGuess = null;
  let bestStats = null;
  let bestParams = null;

  for (let separator of separators) {
    for (let quoteChar of quoteChars) {
      for (let header of headerOptions) {
        const parsedData = parseCSV(text, separator, quoteChar);
        // Skip last row which might be incomplete
        const rows = header? parsedData.slice(1,-1) : parsedData.slice(0,-1);
        if (rows.length > 0) {
          let columnStats = rows[0].map((_, columnIndex) => {
            return rows.reduce((stats, row) => {
              const value = row[columnIndex];
              if (stats.numeric) {
                if (isFinite(value)) {
                  stats.min = Math.min(stats.min, parseFloat(value));
                  stats.max = Math.max(stats.max, parseFloat(value));
                } else {
                  stats.numeric = false;
                }
              }
              if (stats.potentialWKT) {
                if (typeof value !== 'string' || !value.match(/^((MULTI)?(POINT|LINESTRING|POLYGON)|GEOMETRYCOLLECTION)[\s]*\(.*\)/i))
                  stats.potentialWKT = false;
              }
              return stats;
            }, { numeric: true, potentialWKT: true, min: Infinity, max: -Infinity });
          });
          if (!bestGuess || betterStats(columnStats, bestStats)) {
            bestGuess = parsedData;
            bestStats = columnStats;
            bestParams = {header, quoteChar, separator};
          }
        }
      }
    }
  }
  // Inside detectCSVFormat
  if (bestGuess) {
    // Extract rows and headerRow flag from the best guess
    const headerRow = bestParams.header? bestGuess[0] : undefined;
    const rows = bestParams.header? bestGuess.slice(1) : bestGuess;

    const geometryInfo = detectGeometry(headerRow, rows, bestStats);
    return { ...bestParams, ...geometryInfo };
  }

  return null;
};

function parseCSV(csvText, separator = ',', quoteChar = '"') {
  // Define states for the FSM
  const states = {
    START_FIELD: Symbol("startField"),
    IN_QUOTED_FIELD: Symbol("inQuotedField"),
    IN_FIELD: Symbol("inField"),
    END_FIELD: Symbol("endField"),
  };

  let currentState = states.START_FIELD;
  let rows = [[]];
  let currentField = '';

  // Helper function to add the current field to the current row
  function addField() {
    rows[rows.length - 1].push(currentField);
    currentField = '';
    currentState = states.START_FIELD;
  }

  // Helper function to start a new row
  function newRow() {
    rows.push([]);
  }

  // Process each character
  for (let char of csvText) {
    switch (currentState) {
      case states.START_FIELD:
        if (char === quoteChar) {
          currentState = states.IN_QUOTED_FIELD;
        } else if (char === separator) {
          addField();
        } else if (char === '\n') {
          addField();
          newRow();
        } else {
          currentField += char;
          currentState = states.IN_FIELD;
        }
        break;

      case states.IN_FIELD:
        if (char === separator) {
          addField();
        } else if (char === '\n') {
          addField();
          newRow();
        } else {
          currentField += char;
        }
        break;

      case states.IN_QUOTED_FIELD:
        if (char === quoteChar) {
          currentState = states.END_FIELD;
        } else {
          currentField += char;
        }
        break;

      case states.END_FIELD:
        if (char === quoteChar) { // Handle escaped quote
          currentField += char;
          currentState = states.IN_QUOTED_FIELD;
        } else if (char === separator) {
          addField();
        } else if (char === '\n') {
          addField();
          newRow();
          currentState = states.START_FIELD;
        } else {
          // Transition to IN_FIELD if there's no separator after a closing quote
          currentState = states.IN_FIELD;
          currentField += char;
        }
        break;
    }
  }

  // Finalize parsing
  if (currentState !== states.START_FIELD) {
    addField();
  }

  return rows;
}

const betterStats = (stats1, stats2) => {
  // Count the number of numeric and potential WKT columns in each stats object
  const countNumericAndWKT = (stats) => {
    const numericCount = stats.filter(stat => stat.numeric).length;
    const wktCount = stats.filter(stat => stat.potentialWKT).length;
    return { numericCount, wktCount };
  };

  const stats1Counts = countNumericAndWKT(stats1);
  const stats2Counts = countNumericAndWKT(stats2);

  // Compare the counts to determine which stats object is better
  if (stats1Counts.wktCount != 0 && stats2Counts.wktCount == 0) {
    return true;
  } else if (stats1Counts.wktCount == 0 && stats2Counts.wktCount != 0) {
    return false;
  } else {
    return stats1Counts.wktCount > stats2Counts.wktCount;
  }
};


const countNumericColumns = (data) => {
  // Count the number of columns in the first row that are numeric
  return data[0].filter(cell => !isNaN(parseFloat(cell)) && isFinite(cell)).length;
};

const detectGeometry = (headerRow, rows, columnStats) => {
  let geometryType, xColumn, yColumn, wktColumn;

  // Check for WKT geometry
  const wktIndex = columnStats.findIndex(stat => stat.potentialWKT);
  if (wktIndex !== -1) {
    geometryType = 'wkt';
    wktColumn = headerRow ? headerRow[wktIndex] : wktIndex;
  } else {
    // Check for Point geometry (numeric columns)
    const numericColumns = columnStats.map((stat, index) => ({ ...stat, index })).filter(stat => stat.numeric);

    // Identify longitude and latitude columns based on range and naming convention
    numericColumns.forEach(col => {
      const colName = headerRow? headerRow[col.index] : col.index;
      if (colName == "longitude" || colName == "lon" || colName == "x")
        xColumn = colName;
      else if (colName == "latitude" || colName == "lat" || colName == "y")
        yColumn = colName;
      else if (!yColumn && col.max <= 90 && col.min >= -90)
        yColumn = colName;
      else if (!xColumn && col.max <= 180 && col.min >= -180)
        xColumn = colName;
    });

    if (xColumn && yColumn && xColumn != yColumn)
      geometryType = 'point';
  }

  return { "geometry_type": geometryType, "x_column": xColumn, "y_column": yColumn, "wkt_column": wktColumn };
};
