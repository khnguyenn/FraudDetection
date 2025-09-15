import pika
import json
import logging
import joblib
import pandas as pd
import numpy as np
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
RABBITMQ_QUEUE = "fraud_detection_queue"
RESULTS_QUEUE = "fraud_results_queue"

# Model paths
MODEL_PATH = "../artifacts/model.joblib"
PREPROCESSOR_PATH = "../artifacts/preprocessor.joblib"

class FraudDetectionConsumer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.model = None
        self.preprocessor = None
        
    def load_artifacts(self):
        """Load the trained model and preprocessor"""
        try:
            if os.path.exists(MODEL_PATH):
                self.model = joblib.load(MODEL_PATH)
                logger.info(f"Model loaded from {MODEL_PATH}")
            else:
                logger.warning(f"Model file not found at {MODEL_PATH}")
                return False
                
            if os.path.exists(PREPROCESSOR_PATH):
                self.preprocessor = joblib.load(PREPROCESSOR_PATH)
                logger.info(f"Preprocessor loaded from {PREPROCESSOR_PATH}")
            else:
                logger.warning(f"Preprocessor file not found at {PREPROCESSOR_PATH}")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error loading artifacts: {e}")
            return False
    
    def connect_rabbitmq(self):
        """Connect to RabbitMQ"""
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            self.channel = self.connection.channel()
            
            # Declare queues
            self.channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True) # Fraud Detection Queue
            self.channel.queue_declare(queue=RESULTS_QUEUE, durable=True) # Results Queue 
            
            # Set QoS to process one message at a time
            self.channel.basic_qos(prefetch_count=1)
            
            logger.info(f"✅ Connected to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to RabbitMQ: {e}")
            return False

    def engineering_features(self, transaction_data):
        """Engineering features"""
        df = pd.DataFrame([transaction_data])

        # First, create features that depend on TransactionDT BEFORE dropping columns
        df.replace([np.inf, -np.inf], np.nan)    
        df['Day'] = df['TransactionDT']//(60*60*24)
        df['Day'] = df['Day'].astype(int)
        df['TransactionHours'] = (df['TransactionDT']//(60*60))%24
        df['TransactionHours'] = df['TransactionHours'].astype(int)
        df['DayofWeek'] = (df['TransactionDT']//(60*60*24)-1)%7
        df['DayofWeek'] = df['DayofWeek'].astype(int)

        v_cols = ["V" + str(i) for i in range(1, 340) if "V"+str(i) in df.columns ]

        ## V_cols - Now filter columns but keep the new features we just created
        reduced_v_cols = ['V1', 'V3', 'V4', 'V6', 'V8', 'V11', 'V13', 'V14', 'V17', 'V20', 'V23', 'V26', 'V27', 'V30', 'V36', 'V37', 'V40', 'V41', 'V44', 'V47', 'V48', 'V54', 'V56', 'V59', 'V62', 'V65', 'V67', 'V68', 'V70', 'V76', 'V78', 'V80', 'V82', 'V86', 'V88', 'V89', 'V91', 'V96', 
 'V98', 'V99', 'V104', 'V107', 'V108', 'V111', 'V115', 'V117', 'V120', 'V121', 'V123', 'V124', 'V127', 'V129', 'V130', 'V136', 'V138', 'V139', 'V142', 'V147', 'V156', 'V162', 'V165', 'V160', 'V166', 'V178',
'V176', 'V173', 'V182', 'V187', 'V203', 'V205', 'V207', 'V215', 'V169', 'V171', 'V175', 'V180', 'V185', 'V188', 'V198', 'V210', 'V209', 'V218', 'V223', 'V224', 'V226', 'V228', 'V229', 'V235', 'V240', 'V258', 
 'V257', 'V253', 'V252', 'V260', 'V261', 'V264', 'V266', 'V267', 'V274', 'V277', 'V220', 'V221', 'V234', 'V238', 'V250', 'V271', 'V294', 'V284', 'V285', 'V286', 'V291',
 'V297', 'V303', 'V305', 'V307', 'V309', 'V310', 'V320', 'V281', 'V283', 'V289', 'V296', 'V301', 'V314', 'V332', 'V325', 'V335', 'V338']

        # Keep the new time-based features we just created
        v_drop_cols = [col for col in v_cols if col not in v_cols]

        
        # Add D features to keep list
        d_features = ["D"+str(i) for i in range(1,16) if "D"+str(i) in df.columns]
        
        # Only drop columns that we don't need to keep
        df.drop(columns=v_drop_cols, inplace=True)

        amt_whole = [int(str(a).split('.')[0]) for a in df['TransactionAmt'].values]
        amt_decimal = [int(str(a).split('.')[1]) for a in df['TransactionAmt'].values]  
        df['dollars'] = amt_whole
        df['cents'] = amt_decimal

        df['TransactionAmt_log'] = np.log(df['TransactionAmt'].clip(lower=1e-6))

        parent_domain = {'gmail.com':'gmail', 'outlook.com':'microsoft', 
                 'yahoo.com':'yahoo', 'mail.com':'mail', 'anonymous.com':'anonymous', 
                 'hotmail.com':'microsoft', 'verizon.net':'verizon', 'aol.com':'aol', 
                 'me.com':'apple', 'comcast.net':'comcast', 'optonline.net':'optimum', 
                 'cox.net':'cox', 'charter.net':'spectrum', 'rocketmail.com':'yahoo', 
                 'prodigy.net.mx':'AT&T', 'embarqmail.com':'century_link', 'icloud.com':'apple', 
                 'live.com.mx':'microsoft', 'gmail':'gmail', 'live.com':'microsoft', 
                 'att.net':'AT&T', 'juno.com':'juno', 'ymail.com':'yahoo', 
                 'sbcglobal.net':'sbcglobal', 'bellsouth.net':'AT&T', 'msn.com':'microsoft', 
                 'q.com':'century_link','yahoo.com.mx':'yahoo', 'centurylink.net':'century_link',  
                 'servicios-ta.com':'asur','earthlink.net':'earthlink', 'hotmail.es':'microsoft', 
                 'cfl.rr.com':'spectrum', 'roadrunner.com':'spectrum','netzero.net':'netzero', 
                 'gmx.de':'gmx','suddenlink.net':'suddenlink','frontiernet.net':'frontier', 
                 'windstream.net':'windstream','frontier.com':'frontier','outlook.es':'microsoft', 
                 'mac.com':'apple','netzero.com':'netzero','aim.com':'aol', 
                 'web.de':'web_de','twc.com':'whois','cableone.net':'sparklight', 
                 'yahoo.fr':'yahoo','yahoo.de':'yahoo','yahoo.es':'yahoo', 'scranton.edu':'scranton', 
                 'sc.rr.com':'sc_rr','ptd.net':'ptd','live.fr':'microsoft', 
                 'yahoo.co.uk':'yahoo','hotmail.fr':'microsoft','hotmail.de':'microsoft', 
                 'hotmail.co.uk':'microsoft','protonmail.com':'protonmail','yahoo.co.jp':'yahoo'}

        df['P_parent_domain'] = [np.nan if pd.isna(domain) else parent_domain[domain] for domain in df['P_emaildomain']] 
        df['R_parent_domain'] = [np.nan if pd.isna(domain) else parent_domain[domain] for domain in df['R_emaildomain']] 

        def parent_device_name(df):

            if(df['device_name'].isna().all()):
                    return df
                
            df.loc[df['device_name'].str.contains('SM', na=False), 'device_name'] = 'Samsung'
            df.loc[df['device_name'].str.contains('SAMSUNG', na=False), 'device_name'] = 'Samsung'
            df.loc[df['device_name'].str.contains('GT-', na=False), 'device_name'] = 'Samsung'
            df.loc[df['device_name'].str.contains('Moto G', na=False), 'device_name'] = 'Motorola'
            df.loc[df['device_name'].str.contains('Moto', na=False), 'device_name'] = 'Motorola'
            df.loc[df['device_name'].str.contains('moto', na=False), 'device_name'] = 'Motorola'
            df.loc[df['device_name'].str.contains('LG-', na=False), 'device_name'] = 'LG'
            df.loc[df['device_name'].str.contains('rv:', na=False), 'device_name'] = 'RV'
            df.loc[df['device_name'].str.contains('HUAWEI', na=False), 'device_name'] = 'Huawei'
            df.loc[df['device_name'].str.contains('ALE-', na=False), 'device_name'] = 'Huawei'
            df.loc[df['device_name'].str.contains('-L', na=False), 'device_name'] = 'Huawei'
            df.loc[df['device_name'].str.contains('Blade', na=False), 'device_name'] = 'ZTE'
            df.loc[df['device_name'].str.contains('BLADE', na=False), 'device_name'] = 'ZTE'
            df.loc[df['device_name'].str.contains('Linux', na=False), 'device_name'] = 'Linux'
            df.loc[df['device_name'].str.contains('XT', na=False), 'device_name'] = 'Sony'
            df.loc[df['device_name'].str.contains('HTC', na=False), 'device_name'] = 'HTC'
            df.loc[df['device_name'].str.contains('ASUS', na=False), 'device_name'] = 'Asus'
            df.loc[df.device_name.isin(df.device_name.value_counts()[df.device_name.value_counts() < 200].index), 'device_name'] = "Others"

            return df
        
        df['device_name'] = [np.nan if pd.isna(v) else v.split('/')[0] for v in df['DeviceInfo'].values]
        df = parent_device_name(df)

        df['os_name'] = [info if (pd.isna(info)) or (len(info.split())<=1) else ' '.join(info.split()[:-1]) for info in df['id_30']]
        df['os_version'] = [np.nan if (pd.isna(info)) or (len(info.split())<=1) else info.split()[-1] for info in df['id_30']]

        # Process D features while TransactionDT is still available
        d_features = ["D"+str(i) for i in range(1,16) if "D"+str(i) in df.columns]
        for f in d_features:
            df[f] =  df[f] - df.TransactionDT/np.float32(24*60*60)

        df['uid1'] = df['card1'].astype(str)+df['card2'].astype(str)+\
                     df['card3'].astype(str)+df['card5'].astype(str)+\
                     df['card6'].astype(str)+df['addr1'].astype(str)+\
                     df['P_emaildomain'].astype(str)
        df['uid2'] = df['card1'].astype(str)+df['addr1'].astype(str)+\
                            df['P_emaildomain'].astype(str)

        df['uid3'] = df['card1'].astype(str)+ df['addr1'].astype(str) + np.floor(df['Day'] - df['D1']).astype(str)

        # Finally drop TransactionDT after all feature engineering is complete
        df.drop(columns=['TransactionDT'], inplace=True)

        return df
    
    def predict_fraud(self, transaction_data):
        """Predict if transaction is fraudulent using your trained model"""
        try:
            logger.debug("🔧 Step 1: Feature Engineering")
            # Step 1: Apply your feature engineering pipeline
            engineered_df = self.engineering_features(transaction_data)
            logger.debug(f"Features after engineering: {engineered_df.shape}")
            
            logger.debug("🔧 Step 2: Preprocessing")
            # Step 2: Apply your trained preprocessor
            processed_data = self.preprocessor.transform(engineered_df)
            logger.debug(f"Features after preprocessing: {processed_data.shape}")
            
            logger.debug("🔧 Step 3: Model Prediction")
            # Step 3: Make prediction with your trained model
            prediction_proba = self.model.predict_proba(processed_data)[0]
            fraud_probability = prediction_proba[1] if len(prediction_proba) > 1 else prediction_proba[0]
            prediction = 1 if fraud_probability > 0.5 else 0
            
            logger.debug(f"✅ Prediction complete: {prediction}, probability: {fraud_probability:.4f}")
            return prediction, fraud_probability
            
        except Exception as e:
            logger.error(f"❌ Error making prediction: {e}")
            logger.error(f"Error details: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None, None
    
    def process_message(self, ch, method, properties, body):
        """Process incoming transaction message"""
        try:
            # Parse message
            message = json.loads(body)
            transaction_id = message.get('transaction_id', 'unknown')
            transaction_data = message.get('data', {})
            
            logger.info(f"🔍 Processing transaction {transaction_id}")
            
            # Make fraud prediction
            prediction, probability = self.predict_fraud(transaction_data)
            
            if prediction is not None:
                # Prepare result
                result = {
                    'transaction_id': transaction_id,
                    'is_fraud': bool(prediction),
                    'fraud_probability': float(probability)
                }
                
                # Send result to results queue
                self.send_result(result)
                
                # Log result with emoji
                status_emoji = "🚨 FRAUD DETECTED" if prediction == 1 else "✅ LEGITIMATE"
                logger.info(f"{status_emoji} | Transaction {transaction_id} | Probability: {probability:.3f}")
                
            else:
                logger.error(f"❌ Failed to process transaction {transaction_id}")
            
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def send_result(self, result):
        """Send result to results queue"""
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=RESULTS_QUEUE,
                body=json.dumps(result, indent=2),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            logger.debug(f"📤 Result sent for transaction {result['transaction_id']}")
        except Exception as e:
            logger.error(f"❌ Failed to send result: {e}")
    
    def start_consuming(self):
        """Start consuming messages"""
        try:
            self.channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=self.process_message
            )
            
            logger.info(f"🎯 Starting fraud detection consumer...")
            logger.info(f"📨 Listening for messages on '{RABBITMQ_QUEUE}' queue")
            logger.info(f"📤 Results will be sent to '{RESULTS_QUEUE}' queue")
            logger.info("Press CTRL+C to exit")
            
            self.channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("🛑 Consumer stopped by user")
            self.channel.stop_consuming()
        except Exception as e:
            logger.error(f"❌ Error during consumption: {e}")
    
    def close_connection(self):
        """Close RabbitMQ connection"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("🔌 RabbitMQ connection closed")

def main():
    """Main function"""
    consumer = FraudDetectionConsumer()
    
    # Load model artifacts
    if not consumer.load_artifacts():
        logger.error("❌ Cannot start consumer without model artifacts")
        return
    
    # Connect to RabbitMQ
    if not consumer.connect_rabbitmq():
        logger.error("❌ Cannot start consumer without RabbitMQ connection")
        return
    
    try:
        consumer.start_consuming()
    except Exception as e:
        logger.error(f"❌ Consumer error: {e}")
    finally:
        consumer.close_connection()

if __name__ == "__main__":
    main()