const getLogType = data => {
    if (data.EventType) {
        return 'agent-events';
    }

    if (data.AWSContactTraceRecordVersion) {
        return 'ctr-events';
    }

    return;
}

const processRecord = record => {
    const { recordId, data } = record;
    const decodedData = JSON.parse(Buffer.from(data, 'base64').toString('utf-8'));
    
    decodedData.logType = getLogType(decodedData);
    
    if (decodedData.ConnectedToSystemTimestamp && decodedData.DisconnectTimestamp) {
        decodedData.Duration = (new Date(decodedData.DisconnectTimestamp).valueOf() - new Date(decodedData.ConnectedToSystemTimestamp).valueOf())/1000;
    }
    
    const encodedData = Buffer.from(JSON.stringify(decodedData)).toString('base64');
    
    return {
        recordId,
        result: 'Ok',
        data: encodedData
    }
}

exports.handler = async (event) => {
    const {records} = event;
    
    const processedRecords = records.map(processRecord); 
    
    return {records: processedRecords};
};
