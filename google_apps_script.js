function doGet(e) {
    var lock = LockService.getScriptLock();
    lock.tryLock(10000);

    try {
        var jsonString = e.parameter.data;
        if (!jsonString) {
            return createResponse("error", "No data parameter found.");
        }

        var data = JSON.parse(jsonString);
        return processData(data);

    } catch (error) {
        return createResponse("error", error.toString());
    } finally {
        lock.releaseLock();
    }
}

function doPost(e) {
    return createResponse("error", "Please use the browser-based Export link (GET request).");
}

function processData(data) {
    var sprintKey = getSprintKey(data.sprintName);
    if (!sprintKey) {
        return createResponse("error", "Could not parse sprint key (e.g. IR21) from name: " + data.sprintName);
    }

    var sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Team Stats");
    if (!sheet) {
        return createResponse("error", "Sheet 'Team Stats' not found.");
    }

    // 1. VELOCITY STATS (Lookup A4:A26)
    // Key in Col A. Update B (Unplanned), C (Planned), D (Velocity)
    updateVerticalRange(sheet, "A4:A26", sprintKey, 4, function (row) {
        // B, C, D are columns 2, 3, 4
        sheet.getRange(row, 2).setValue(data.completedUnplanned);
        sheet.getRange(row, 3).setValue(data.completedPlanned);
        sheet.getRange(row, 4).setValue(data.velocity);
        sheet.getRange(row, 2, 1, 3).setBackground("#fff2cc");
    });

    // 2. TASK COMPLETION STATS (Lookup A34:A56)
    // Key in Col A. Update B (Tasks Done), C (Tasks Incomplete), D (Carryover %)
    updateVerticalRange(sheet, "A34:A56", sprintKey, 34, function (row) {
        sheet.getRange(row, 2).setValue(data.completedTasks);
        sheet.getRange(row, 3).setValue(data.incompleteTasks);
        sheet.getRange(row, 4).setValue(data.carryover / 100); // Input is usually %, Sheets needs 0.xx
        sheet.getRange(row, 2, 1, 3).setBackground("#fff2cc");
    });

    // 3. COMPLETION PERCENTAGES (Lookup G3:AC3)
    // Key in Row 3. Update Row 6, 7, 8, 9 in found Column.
    updateHorizontalRange(sheet, "G3:AC3", sprintKey, 7, function (col) { // 7 = G
        sheet.getRange(6, col).setValue(data.plannedPct / 100);
        sheet.getRange(7, col).setValue(data.plannedCompletionPct / 100);
        sheet.getRange(8, col).setValue(data.unplannedCompletionPct / 100);
        sheet.getRange(9, col).setValue(data.totalCompletionPct / 100);
        sheet.getRange(6, col, 4, 1).setBackground("#fff2cc");
    });

    // 4. TASK VS SP COMPLETION (Lookup G24:AC24)
    // Key in Row 24. Update Row 25 (Task %), 26 (Total %)
    updateHorizontalRange(sheet, "G24:AC24", sprintKey, 7, function (col) {
        sheet.getRange(25, col).setValue(data.taskCompletionPct / 100);
        sheet.getRange(26, col).setValue(data.totalCompletionPct / 100); // Duplicate of above? Following spec.
        sheet.getRange(25, col, 2, 1).setBackground("#fff2cc");
    });

    // 5. BUGS IN VS BUGS OUT (Lookup G39:AC39)
    // Key in Row 39. Update Row 40 (In), 41 (Out)
    updateHorizontalRange(sheet, "G39:AC39", sprintKey, 7, function (col) {
        sheet.getRange(40, col).setValue(data.bugsIn);
        sheet.getRange(41, col).setValue(data.bugsOut);
        sheet.getRange(40, col, 2, 1).setBackground("#fff2cc");
    });

    // 6. LATEST DATA (Static Mapping)
    // B62: Planned SP, C62: Completed Planned SP
    sheet.getRange("B62").setValue(data.plannedSP);
    sheet.getRange("C62").setValue(data.completedPlanned);

    // E62: Unplanned SP, F62: Completed Unplanned SP
    sheet.getRange("E62").setValue(data.unplannedSP);
    sheet.getRange("F62").setValue(data.completedUnplanned);

    // L62: Bugs In, M62: Bugs Out
    sheet.getRange("L62").setValue(data.bugsIn);
    sheet.getRange("M62").setValue(data.bugsOut);

    // No highlighting for static data as requested.

    SpreadsheetApp.flush();
    return createResponse("success", "Updated stats for " + sprintKey);
}

// Helper: Vertical Lookup
// rangeAddress: e.g. "A4:A26"
// startRow: e.g. 4 (the row index of the first cell in range)
function updateVerticalRange(sheet, rangeAddress, key, startRow, callback) {
    var range = sheet.getRange(rangeAddress);
    var values = range.getValues(); // Array of [ [val], [val] ]

    for (var i = 0; i < values.length; i++) {
        if (values[i][0] == key) {
            var actualRow = startRow + i;
            callback(actualRow);
            return;
        }
    }
}

// Helper: Horizontal Lookup
// rangeAddress: e.g. "G3:AC3"
// startCol: e.g. 7 (G is 7th column)
function updateHorizontalRange(sheet, rangeAddress, key, startCol, callback) {
    var range = sheet.getRange(rangeAddress);
    var values = range.getValues()[0]; // Single row array

    for (var i = 0; i < values.length; i++) {
        if (values[i] == key) {
            var actualCol = startCol + i;
            callback(actualCol);
            return;
        }
    }
}

function getSprintKey(sprintName) {
    try {
        // 1. Check for Iteration X
        var match = sprintName.match(/Iteration\s+(\d+)/i);
        if (match && match[1]) {
            var num = parseInt(match[1], 10);
            var padded = num < 10 ? '0' + num : '' + num;
            return 'IR' + padded;
        }
        // 2. Fallback: If name IS strict format "IR23" (case insensitive)
        if (/^IR\d+$/i.test(sprintName)) return sprintName.toUpperCase();

        return null;
    } catch (e) {
        return null;
    }
}

function createResponse(status, message) {
    var output = { status: status, updated: message };
    return ContentService.createTextOutput(JSON.stringify(output)).setMimeType(ContentService.MimeType.JSON);
}
