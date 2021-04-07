const AWS = require('aws-sdk');
const https = require('https');

const S3 = new AWS.S3();

const EVENT_TYPE = 'ConversationAnalysisSample';
const TRANSCRIPTION_EVENT_TYPE = 'ConversationTranscriptSample';

const buildTranscriptEvents = ({Transcript, CustomerMetadata}) => {
    return Transcript.map(t => ({ 
        ...t,
        eventType: TRANSCRIPTION_EVENT_TYPE,
        ContactId: CustomerMetadata.ContactId
    }));
}

const buildNewRelicEvent = payload => {
    const { 
        AccountId, 
        Channel, 
        CustomerMetadata, 
        LanguageCode, 
        Participants, 
        Categories,
        ConversationCharacteristics
    } = payload;
    
    const {
        TotalConversationDurationMillis,
        Interruptions,
        NonTalkTime,
        TalkSpeed,
        TalkTime,
        Sentiment
    } = ConversationCharacteristics;
    
    const event = {
        "eventType": EVENT_TYPE,
        "AwsAccountId": AccountId,
        "Channel": Channel,
        "ContactId": CustomerMetadata.ContactId,
        "LanguageCode": LanguageCode,
        "Conversation.NumberOfParticipants": Participants.length,
        "Categories.MatchedCategories": Categories.MatchedCategories,
        "Categories.MatchedDetails": Categories.MatchedDetails,
        "Conversation.TotalDuration": TotalConversationDurationMillis,
        "Conversation.Interruptions.TotalCount": Interruptions.TotalCount,
        "Conversation.Interruptions.TotalTimeMillis": Interruptions.TotalTimeMillis,
        "Conversation.NonTalkTime.TotalTimeMillis": NonTalkTime.TotalTimeMillis,
        "Conversation.NonTalkTime.NumberOfInstances": NonTalkTime.Instances.length,
        "Conversation.TalkTime.TotalTimeMillis": TalkTime.TotalTimeMillis
    }
    
    Object.keys(Sentiment.OverallSentiment).forEach(participant => {
        event[`Conversation.Sentiment.${participant}`] = Sentiment.OverallSentiment[participant];
    })
    
    Object.keys(Sentiment.SentimentByPeriod.QUARTER).forEach(participant => {
        Sentiment.SentimentByPeriod.QUARTER[participant].forEach((period,i) => {
            event[`Conversation.Sentiment.${participant}.Q${i+1}.BeginOffsetMillis`] = period.BeginOffsetMillis;
            event[`Conversation.Sentiment.${participant}.Q${i+1}.EndOffsetMillis`] = period.EndOffsetMillis;
            event[`Conversation.Sentiment.${participant}.Q${i+1}`] = period.Score;
        })
    })
    
    Object.keys(TalkSpeed.DetailsByParticipant).forEach(participant => {
        event[`Conversation.TalkSpeed.${participant}`] = TalkSpeed.DetailsByParticipant[participant].AverageWordsPerMinute;
    })
    
    Object.keys(payload.ConversationCharacteristics.TalkTime.DetailsByParticipant).forEach(participant => {
        event[`Conversation.TalkTime.${participant}`] = TalkTime.DetailsByParticipant[participant].TotalTimeMillis;
    })
    
    return event;
}

const processRecord = async record => {
    const { object, bucket } = record.s3;
    const Bucket = decodeURIComponent(bucket.name);
    const Key = decodeURIComponent(object.key);

    const s3Object = await S3.getObject({Bucket, Key}).promise()
        .then(({Body}) => {
            return JSON.parse(Body.toString('utf8'));
        });
        
    const analysisEvent = buildNewRelicEvent(s3Object);
    const transcriptEvents = buildTranscriptEvents(s3Object);

    return ([analysisEvent, ...transcriptEvents]);
}

const sendEventsToNewRelic = (eventsPayload) => {
    return new Promise((resolve,reject) => {
        if (!eventsPayload) {
            resolve();
        }
        
        const data = JSON.stringify(eventsPayload)
    
        const options = {
          hostname: 'insights-collector.newrelic.com',
          port: 443,
          path: `/v1/accounts/${process.env.NR_ACCOUNT_ID}/events`,
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Content-Length': data.length,
            'x-insert-key': process.env.NR_API_KEY
          }
        }
        
        const req = https.request(options, res => {
            if (parseFloat(res.statusCode) > 399) {
                console.log(`statusCode: ${res.statusCode}`);
            }
            res.on('end', () => resolve(body));
        })
        
        req.on('error', error => {
            console.log("http error:", JSON.stringify(error));
            reject(error);
        })
        
        req.write(data);
        req.end();
        
    })
}

exports.handler = async (event) => {
    if (!process.env.NR_ACCOUNT_ID) {
      throw "NR_ACCOUNT_ID not set";
    }

    if (!process.env.NR_API_KEY) {
        throw "NR_API_KEY not set";
    }
    
    const NewRelicPayload = await Promise.all(event.Records.map(processRecord))
        .then(data => [].concat.apply([], (data || [])));
    
    if (!event.isTest) {
        await sendEventsToNewRelic(NewRelicPayload);
    }
    
    return {
        statusCode: 200
    };
};
