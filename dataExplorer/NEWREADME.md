# BEAST DataExplorer Extension

## Overview

This project extends the BEAST DataExplorer by integrating LLM functionality using the Gemini API. The integration adds AI suggestions, AI styling, and AI-based datetime parsing features to enhance visualization and dataset export capabilities. In this project, several core files have been modified or added to support these changes.

## Prerequisites

- **IDE**: IntelliJ IDEA
- **Build Tool**: Maven (ensure that Maven is installed and available)
- **JDK/SDK**: Use the SDK configured for the DataExplorer project
- **Git**: The project was forked from the BEAST DataExplorer repository at version `0.10.1-RC2`

## How to Compile, Deploy, and Run

1. **Load the Project**:
    - Open IntelliJ IDEA.
    - Import the project as a Maven project.

2. **Maven Setup**:
    - Once loaded, update resources from the `pom.xml` file by right-clicking the project and selecting **Maven > Reload Project**.

3. **Run Configuration**:
    - Set the main configuration to use the DataExplorer SDK.
    - In your run configuration, set the program arguments to:
      ```
      server -enableStaticFileHandling
      ```

4. **Gemini API Integration**:
    - Open the file located at `src/main/resources/dataExplorer/utils/app.js`.
    - In the following functions, replace the placeholder API key with your actual Gemini API key:
        - `callLLMForStyling`
        - `callLLMForSuggestions`
        - `callLLM`

5. **Deploy and Run**:
    - Run the main configuration.
    - The server should start and be accessible via the configured URL.

## Key Modifications Made

### DataProcessor.scala
- **Refactoring**: Transitioned from using SpatialRDDs to DataFrames.
- **Core Changes**: Modified methods including:
    - `loadRawData`
    - `summarizeData` – now calculates several statistics for numeric, string, and boolean data types.
    - `buildIndexes`

### DatasetProcessor.scala
- **Export Dataset Enhancements**:
    - Added support for a preview option in the `exportDataset` method so users can request a JSON preview of a dataset.
    - Introduced an optional `limit` parameter in the `exportDataset` method to restrict the number of records in the preview.

### Frontend Changes (app.js and custom_styles.js)
- **New AI Features**:
    - Integrated AI suggestions and styling functions.
    - Added AI-based datetime parsing capabilities.
- **Note**: The "save all suggested styles" button in the AI suggestions section is non-functional and should be removed.

## Future Work and Suggestions

1. **Modularize app.js**: Break down the single, large `app.js` file into several smaller files for better manageability.
2. **LLM Function Consolidation**: Merge the multiple LLM call functions into a single function with distinct flows for each use-case.
3. **API Key Security**: Implement enhanced security measures to protect the Gemini API key.
4. **Geometry Data Handling**: Address the current lack of proper handling for geometry data types (e.g., for calculating MBR) in the `summarizeData` method.
5. **Frontend Enhancements**: Utilize existing metadata (like threshold-based filters) to further improve the frontend visualization.
6. **Dynamic Datetime Filtering**: Add backend support for dynamic datetime filtering (for example, using a slider to change the displayed data).

## Git Version and Modified Files

- **Forked Repository Version**: BEAST DataExplorer version `0.10.1-RC2`
- **Files Modified or Added**:
    - `DataProcessor.scala` – Core changes in loading, summarizing, and indexing data.
    - `DatasetProcessor.scala` – Enhancements in the export functionality.
    - `app.js` – Integration of AI suggestions, AI styling, and AI-based datetime parsing.
    - `custom_styles.js` – Updates to support the new AI styling functions.
