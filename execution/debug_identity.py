import os
import boto3
from dotenv import load_dotenv

load_dotenv()

def check_identity_and_policies():
    try:
        session = boto3.Session(
            aws_access_key_id=os.getenv("SP_API_AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SP_API_AWS_SECRET_KEY"),
            region_name=os.getenv("SP_API_REGION", "us-east-1")
        )
        iam = session.client("iam")
        sts = session.client("sts")
        
        # 1. Get Identity
        identity = sts.get_caller_identity()
        user_arn = identity['Arn']
        user_name = user_arn.split("/")[-1]
        print(f"Authenticated as User: {user_name} ({user_arn})")
        
        # 2. Try to List Policies (This might fail if the user doesn't have IAM read permissions)
        print(f"Checking attached policies for {user_name}...")
        try:
            response = iam.list_attached_user_policies(UserName=user_name)
            policies = response.get('AttachedPolicies', [])
            if not policies:
                print("No managed policies attached directly to user.")
            else:
                print("Attached Policies:")
                for p in policies:
                    print(f" - {p['PolicyName']} (ARN: {p['PolicyArn']})")
                    
            # Also check inline policies
            response_inline = iam.list_user_policies(UserName=user_name)
            inline_policies = response_inline.get('PolicyNames', [])
            if not inline_policies:
                print("No inline policies found.")
            else:
                print("Inline Policies:")
                for p in inline_policies:
                    print(f" - {p}")
                    
        except Exception as e:
            print(f"Could not list policies (Permission Error?): {e}")

    except Exception as e:
        print(f"Fatal Error: {e}")

if __name__ == "__main__":
    check_identity_and_policies()
