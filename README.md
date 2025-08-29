import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re

# Sample dataset with 20 rows
sample_data = {
    'Concern_Reason': [
        'Small Business Merchant Services', 'Small Business Merchant Services', 'Account Loan Issue', 'Account Loan Issue',
        'Online Banking Desktop', 'Online Banking Desktop', 'Credit Card Issue', 'Credit Card Issue',
        'Small Business Merchant Services', 'Account Loan Issue', 'Online Banking Mobile', 'Credit Card Issue',
        'Small Business Merchant Services', 'Account Loan Issue', 'Online Banking Desktop', 'Credit Card Issue',
        'Small Business Merchant Services', 'Online Banking Mobile', 'Account Loan Issue', 'Credit Card Issue'
    ],
    'Detail_Reason': [
        'Language Services', 'Equipment Functionality', 'Credit Decision Application', 'Account Opening Issue',
        'Login Issues', 'Transaction History', 'Payment Processing', 'Rewards Program',
        'Transaction Settlement', 'Credit Limit Increase', 'Mobile App Crash', 'Billing Dispute',
        'Language Services', 'Account Closure Request', 'Password Reset', 'Fraud Alert',
        'Equipment Functionality', 'Balance Inquiry', 'Credit Decision Application', 'Payment Processing'
    ],
    'Additional_Detail': [
        'Spanish Translation', 'Terminal Not Working', 'New Application Decline', 'Documentation Missing',
        'Cannot Access Account', 'Statement Not Available', 'Declined Transaction', 'Points Not Credited',
        'Funds Not Received', 'Increase Request Denied', 'App Won\'t Open', 'Incorrect Charge',
        'French Translation', 'Account Closure Pending', 'Forgot Password', 'Suspicious Activity',
        'Card Reader Issue', 'Check Balance Failed', 'Existing Customer Decline', 'Transaction Failed'
    ],
    'Additional_Sub_Data': [
        'Business Banking', 'Merchant Terminal', 'Credit Score Too Low', 'Income Verification',
        'Login Credentials', 'PDF Generation Error', 'Insufficient Funds', 'Reward Calculation',
        'Settlement Delay', 'Debt to Income Ratio', 'iOS Version Issue', 'Merchant Error',
        'Business Banking', 'Final Notice Sent', 'Security Questions', 'Card Blocked',
        'Hardware Malfunction', 'Server Connection', 'Previous Bankruptcy', 'Network Timeout'
    ],
    'FLU_Assignment': [
        'Merchant Services', 'Merchant Services', 'Consumer Credit', 'Consumer Banking',
        'Digital Banking', 'Digital Banking', 'Consumer Credit', 'Consumer Credit',
        'Merchant Services', 'Consumer Credit', 'Digital Banking', 'Consumer Credit',
        'Merchant Services', 'Consumer Banking', 'Digital Banking', 'Consumer Credit',
        'Merchant Services', 'Digital Banking', 'Consumer Credit', 'Consumer Credit'
    ]
}

# Create DataFrame
df = pd.DataFrame(sample_data)
print("Sample Dataset:")
print(df.head(10))
print(f"\nDataset shape: {df.shape}")
print(f"Unique FLU Assignments: {df['FLU_Assignment'].unique()}")

def preprocess_text(text):
    """Clean and preprocess text"""
    text = str(text).lower()
    text = re.sub(r'[^a-zA-Z\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def create_document(row):
    """Combine all text fields into a single document"""
    fields = [
        row['Concern_Reason'], 
        row['Detail_Reason'], 
        row['Additional_Detail'], 
        row['Additional_Sub_Data']
    ]
    combined = ' '.join([str(field) for field in fields])
    return preprocess_text(combined)

# Create documents from historical data
df['Document'] = df.apply(create_document, axis=1)
historical_documents = df['Document'].tolist()
historical_flu_assignments = df['FLU_Assignment'].tolist()

print("\nSample Documents:")
for i in range(3):
    print(f"Doc {i+1}: {historical_documents[i]}")
    print(f"FLU: {historical_flu_assignments[i]}\n")

class IRFLUAssigner:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 2),  # Include bigrams
            min_df=1,
            max_df=0.95
        )
        self.historical_documents = None
        self.historical_assignments = None
        self.tfidf_matrix = None
        
    def fit(self, documents, assignments):
        """Fit the IR system on historical data"""
        self.historical_documents = documents
        self.historical_assignments = assignments
        
        # Create TF-IDF matrix
        self.tfidf_matrix = self.vectorizer.fit_transform(documents)
        print(f"TF-IDF Matrix shape: {self.tfidf_matrix.shape}")
        
        # Show top features
        feature_names = self.vectorizer.get_feature_names_out()
        print(f"Sample features: {feature_names[:10]}")
        
    def predict(self, query_text, top_k=5):
        """Find most similar documents and predict FLU assignment"""
        
        # Preprocess query
        query_processed = preprocess_text(query_text)
        
        # Transform query to TF-IDF vector
        query_vector = self.vectorizer.transform([query_processed])
        
        # Calculate cosine similarity with all historical documents
        similarities = cosine_similarity(query_vector, self.tfidf_matrix)[0]
        
        # Get top k most similar documents
        top_indices = np.argsort(similarities)[::-1][:top_k]
        top_similarities = similarities[top_indices]
        
        # Show similar documents
        print(f"\nQuery: {query_processed}")
        print(f"\nTop {top_k} most similar historical cases:")
        print("-" * 80)
        
        similar_assignments = []
        for i, (idx, sim_score) in enumerate(zip(top_indices, top_similarities)):
            assignment = self.historical_assignments[idx]
            document = self.historical_documents[idx]
            similar_assignments.append((assignment, sim_score))
            
            print(f"Rank {i+1} (Similarity: {sim_score:.4f})")
            print(f"Document: {document}")
            print(f"FLU Assignment: {assignment}")
            print("-" * 40)
        
        # Weighted voting based on similarity scores
        assignment_scores = {}
        total_weight = 0
        
        for assignment, score in similar_assignments:
            if score > 0:  # Only consider positive similarities
                assignment_scores[assignment] = assignment_scores.get(assignment, 0) + score
                total_weight += score
        
        if assignment_scores:
            # Normalize scores
            for assignment in assignment_scores:
                assignment_scores[assignment] /= total_weight
            
            # Get prediction
            predicted_assignment = max(assignment_scores, key=assignment_scores.get)
            confidence = assignment_scores[predicted_assignment]
            
            print(f"\nWeighted Voting Results:")
            for assignment, score in sorted(assignment_scores.items(), key=lambda x: x[1], reverse=True):
                print(f"  {assignment}: {score:.4f}")
            
            return predicted_assignment, confidence, assignment_scores
        else:
            return "Unknown", 0.0, {}

# Initialize and train the IR system
ir_assigner = IRFLUAssigner()
ir_assigner.fit(historical_documents, historical_flu_assignments)

print("\n" + "="*100)
print("DEMONSTRATION - PREDICTING NEW CASES")
print("="*100)

# Test cases
test_cases = [
    "Small Business Merchant Services Language Services Portuguese Translation Business Banking",
    "Credit Card Issue Payment Processing Transaction Failed Network Error", 
    "Online Banking Mobile Balance Inquiry Server Connection Failed"
]

for i, test_case in enumerate(test_cases, 1):
    print(f"\n🔍 TEST CASE {i}:")
    prediction, confidence, scores = ir_assigner.predict(test_case, top_k=3)
    print(f"\n✅ FINAL PREDICTION: {prediction} (Confidence: {confidence:.4f})")
    print("="*100)
