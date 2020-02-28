'use strict';

/*
importamos las bibliotecas
el sdk que usaremos para
acceder a cloudwatch y aws kinesis
*/

var AWS = require('aws-sdk');
var agg = require('aws-kinesis-agg');

console.log('Loading function');


/*
defino un controlador que será llamado por aws cloudwatch
para procesar nuestros registros de Kinesis.
*/
exports.handler = (event, context, callback) => {

    var cloudwatch = new AWS.CloudWatch();

    console.log('Processing records: ', event.Records.length);
    console.log('Received event:', JSON.stringify(event, null, 2));


    /*
        en este controlador itero sobre cada registro de kinesis

    */
    event.Records.forEach((record) => {
        console.log('Processing records: ', record.kinesis.sequenceNumber);

        // Desagregar cada registro KPL
        agg.deaggregateSync(record.kinesis, true, (err, userRecords) => {
            console.log('Deaggregating records: ' + userRecords.length);
            if (err) {
                console.log(err);
                callback(err)
                return;
            }

            // Iterar sobre los tweets
            // podemos iterar sobre los registros de usuario
            userRecords.forEach((record) => {


                // Obtener tweet serializado
                // los registros que llegan están en formato Base64
                // antes de analizar necesito convertirlos a ASCII
                // para luego analizar JSON
                var tweetData = new Buffer(record.data, 'base64');
                
                // Parse a tweet
                /*
                ¿Porqué hacer esto?
                Los tweets están escritos en formato JSON y necesito
                extraer los datos necesarios como por ejemplo:
                -- el momento en que se publicó
                 */
                var tweet = JSON.parse(tweetData.toString('ascii'))

                console.log('Processing tweet:', tweet)
                console.log('Tweet created at: ', tweet.created_at);

                // Almacenar el valor 1.0 por cada tweet creado

                // https://docs.aws.amazon.com/cli/latest/reference/cloudwatch/put-metric-data.html
                /*
                MetricData: Nos permite enviar datos de métricas a Cloudwatch
                */
                var params = {

                    //matriz de registos métricos
                    MetricData: [
                        {
                            MetricName: 'tweets-count',             // cada registro de métrica tiene un MetricName
                            Timestamp: tweet.timestamp_ms / 1000,   // cada registro de métrica tiene una marca de tiempo
                            Unit: "None",                           // necesito proporcionar una unidad (en este caso es ninguno, sin embargo puede ser definido en segundos o bytes)
                            Value: 1.0                              // valor métrico que se almacena
                        }
                    ],
                    Namespace: 'sodsean-kinesis'
                };

                // Escribir datos en CloudWatch
                
                cloudwatch.putMetricData(params, function(err, data) {

                    /*
                    proporcionaremos la función de llamada
                    cuando tengamos acceso  a la API*/

                    if (err) {
                        console.log(err, err.stack);
                        callback(err);
                    }
                });
            });
        });
    });
    callback(null, `Successfully processed ${event.Records.length} records.`);

    /*
    KPL => KINESIS PRODUCER LIBRARY

    Un productor de Amazon Kinesis Data Streams es 
    una aplicación que coloca los registros de datos 
    del usuario en un flujo de datos de Kinesis 
    (también llamado ingestión de datos ). 
    
    La Kinesis Producer Library (KPL) simplifica el 
    desarrollo de aplicaciones de productores, lo que 
    permite a los desarrolladores lograr un alto rendimiento 
    de escritura en un flujo de datos de Kinesis.
    */

    /*
    API => Application Programming Interface

    Es un conjunto de funciones y procedimientos que cumplen
    una o muchas funciones con el fin de ser utilizadas por 
    otro software.
    */
};
