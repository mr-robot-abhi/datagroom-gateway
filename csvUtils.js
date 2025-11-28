const Fs = require('fs');
const DbAbstraction = require('./dbAbstraction');
const CsvReadableStream = require('csv-reader');
const logger = require('./logger');

class CsvUtils {

    static async findHdrs (file) {
        return new Promise((resolve, reject) => {
            let inputStream = Fs.createReadStream(file, 'utf8');
             inputStream
                .pipe(new CsvReadableStream({ parseNumbers: true, parseBooleans: true, trim: true }))
                .on('data', function (row) {
                    logger.info(row, "A row data arrived");
                })
                .on('end', function (data) {
                    logger.info('No more rows!');
                })
                .on ('header', function (hdr) {
                    logger.info(hdr, 'Header');
                    inputStream.destroy();
                    resolve(hdr);
                });
        });   
    }

    // Auto-detect CSV file information (row count, column count)
    static async autoDetectRangeInfo (file) {
        return new Promise((resolve, reject) => {
            let dataRowCount = 0; // Count of data rows (excluding header)
            let columnCount = 0;
            let hdrs = [];
            let inputStream = Fs.createReadStream(file, 'utf8');
            
            inputStream
                .pipe(new CsvReadableStream({ parseNumbers: true, parseBooleans: true, trim: true }))
                .on('data', function (row) {
                    dataRowCount++;
                    // Update column count if this row has more columns
                    if (Array.isArray(row) && row.length > columnCount) {
                        columnCount = row.length;
                    }
                })
                .on('end', function (data) {
                    // Total rows = header row + data rows
                    let totalRowCount = dataRowCount + (hdrs.length > 0 ? 1 : 0);
                    logger.info(`Auto-detected CSV info: ${totalRowCount} total rows (${dataRowCount} data rows), ${columnCount} columns`);
                    resolve({
                        status: true,
                        rowCount: totalRowCount,
                        dataRowCount: dataRowCount,
                        columnCount: columnCount,
                        headers: hdrs
                    });
                })
                .on('header', function (hdr) {
                    logger.info(hdr, 'Header');
                    hdrs = hdr;
                    if (Array.isArray(hdr) && hdr.length > columnCount) {
                        columnCount = hdr.length;
                    }
                })
                .on('error', function (error) {
                    logger.error(error, "Error reading CSV file");
                    reject({ status: false, error: error.message || 'Error reading CSV file' });
                });
        });
    }

    static async loadDataIntoDb (file, keys, dsName, dsUser) {

        return new Promise(async (resolve, reject) => {
            logger.info(`File: ${file} keys: ${keys} dsName: ${dsName} dsUser: ${dsUser}`);
            let dbAbstraction = new DbAbstraction();
            // Check if db already exists...
            let dbList = await dbAbstraction.listDatabases();
            for (let i = 0; i < dbList.length; i++) {
                if (dbList[i].name === dsName) {
                    logger.warn(`${dsName} Dataset name conflict`);
                    reject ({ loadStatus: false, error: 'Dataset name conflict' });
                    return;
                }
            }
            let hdrs = [];
            let inputStream = Fs.createReadStream(file, 'utf8');
             inputStream
                .pipe(new CsvReadableStream({ asObject: true, parseNumbers: true, parseBooleans: true, trim: true }))
                .on('data', async function (rowObjForDb) {
                    // From here on, you can insert the rows into database. 
                    logger.info(rowObjForDb, "Row object");
                    let rowSelectorObj = {};
                    keys.map((k) => {
                        rowSelectorObj[k] = rowObjForDb[k];
                    });
                    logger.info(rowSelectorObj, "Row selector obj");
                    try {
                        await dbAbstraction.update(dsName, "data", rowSelectorObj, rowObjForDb);
                    } catch (e) {
                        logger.error(e, "Db update error in loadDataInDb");
                    }
                })
                .on ('header', function (h) {
                    logger.info(h, 'Header');
                    hdrs = h;
                })
                .on('end', async function (data) {
                    // Do meta data stuff here. 
                    try {
                        await dbAbstraction.update(dsName, "metaData", { _id: "perms" }, { owner: dsUser });
                        await dbAbstraction.update(dsName, "metaData", { _id: "keys" }, { keys });
                        let columns = {};
                        let columnAttrs = [];
                        for (let i = 0; i < Object.keys(hdrs).length; i++) {
                            let attrs = {};
                            attrs.field = hdrs[i];
                            attrs.title = hdrs[i];
                            attrs.width = 150;
                            attrs.editor = "textarea";
                            attrs.editorParams = {};
                            attrs.formatter = "textarea";
                            attrs.headerFilterType = "input";
                            attrs.hozAlign = "center";
                            attrs.vertAlign = "middle";
                            attrs.headerTooltip = true;
                            columnAttrs.push(attrs);
                            columns[i + 1] = hdrs[i];
                        }
                        await dbAbstraction.update(dsName, "metaData", { _id: `view_default` }, { columns, columnAttrs, userColumnAttrs: { } } );
                    } catch (e) {
                        logger.error(e, "Db metaData update error");
                    }            
                    resolve ({ loadStatus: true, hdrs })
                });

        });   
    }

}

module.exports = CsvUtils;