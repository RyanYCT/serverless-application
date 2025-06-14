import decimal
import json
import logging
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert Decimal to int/float for JSON serialization"""

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            # If the decimal is whole number, convert to int
            if o % 1 == 0:
                return int(o)
            # Else convert to float
            else:
                return float(o)

        return super(DecimalEncoder, self).default(o)


class LambdaRouter:
    """Router class to handle different types of Lambda events"""

    def __init__(self):
        self.api_routes = {}
        self.step_routes = {}
        self.default_headers = {"Content-Type": "application/json"}

    def api_route(self, method: str, path: str):
        """Decorator to register API Gateway routes"""
        route_key = f"{method}:{path}"

        def decorator(func):
            self.api_routes[route_key] = func
            return func

        return decorator

    def step_route(self, step_name: str):
        """Decorator to register Step Functions routes"""

        def decorator(func):
            self.step_routes[step_name] = func
            return func

        return decorator

    def handle(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Main handler that routes requests based on the event source"""
        logger.info(f"Event received: {json.dumps(event)}")

        # Determine the event source and route
        try:
            # API Gateway event
            if "httpMethod" in event or "requestContext" in event:
                return self._handle_api_gateway(event)

            # Step Functions event or default
            else:
                return self._handle_step_function(event)

        except Exception as e:
            logger.error(f"Error processing event: {e}")
            # For API Gateway, return HTTP error
            if "httpMethod" in event or "requestContext" in event:
                return {
                    "statusCode": 500,
                    "headers": self.default_headers,
                    "body": json.dumps({"error": str(e)}),
                }
            # For Step Functions, return plain error
            else:
                return {"error": str(e)}

    def _handle_api_gateway(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle API Gateway events by routing to the appropriate handler"""
        # Extract HTTP method and path from the event
        if "requestContext" in event and "http" in event["requestContext"]:
            # HTTP API format
            http_method = event["requestContext"]["http"]["method"]
            resource_path = event["requestContext"]["http"]["path"]
        else:
            # REST API format
            http_method = event.get("httpMethod")
            resource_path = event.get("resource", event.get("path", ""))

        # Create the route key
        route_key = f"{http_method}:{resource_path}"

        # Find the handler function
        handler_func = self.api_routes.get(route_key)

        if not handler_func:
            # Try to match parameterized routes
            for config_route, config_handler in self.api_routes.items():
                if self._is_route_match(config_route, route_key):
                    handler_func = config_handler
                    break

        if handler_func:
            return handler_func(event)
        else:
            logger.error(f"No handler found for route: {route_key}")
            return {
                "statusCode": 404,
                "headers": self.default_headers,
                "body": json.dumps({"error": f"Route not found: {route_key}"}),
            }

    def _handle_step_function(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle Step Functions events by routing to the appropriate handler"""
        # Extract the step name from the event if available
        step_name = event.get("step")

        # Find the handler function
        handler_func = self.step_routes.get(step_name)
        if not handler_func:
            # Try default handler if step name is not specified
            handler_func = self.step_routes.get("default")

        if handler_func:
            return handler_func(event)
        else:
            logger.error(f"Invalid step name: {step_name}")
            return {"error": f"Invalid step name: {step_name}"}

    def _is_route_match(self, config_route: str, actual_route: str) -> bool:
        """Check if a parameterized route matches the actual route"""
        # Split method and path
        config_method, config_path = config_route.split(":", 1)
        actual_method, actual_path = actual_route.split(":", 1)

        # Methods must match
        if config_method != actual_method:
            return False

        # Check path matching
        config_parts = config_path.split("/")
        actual_parts = actual_path.split("/")

        if len(config_parts) != len(actual_parts):
            return False

        for i, part in enumerate(config_parts):
            if "{" in part and "}" in part:
                # Skip parameter
                continue
            elif part != actual_parts[i]:
                return False

        return True


class DataCleanerService:
    """Service class for data cleaning operations"""

    def __init__(self):
        self.s3_client = boto3.client("s3")

    def process_data(self, data: List) -> List[Dict[str, Any]]:
        """
        Process and clean the data from the API response
        """
        if not data:
            logger.error("Empty data received")
            return []

        try:
            flattened_data = []
            for sublist in data:
                for item in sublist:
                    flattened_data.append(item)

            # Add timestamp and remove redundant attributes
            keys_to_remove = ["minEnhance", "maxEnhance", "basePrice", "priceMin", "priceMax"]
            for item in flattened_data:
                for key in keys_to_remove:
                    item.pop(key, None)

            logger.info(f"Processed {len(flattened_data)} items")
            return flattened_data

        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise


# Initialize router
router = LambdaRouter()

# Initialize services
data_cleaner = DataCleanerService()


# API Gateway route handlers (placeholder for extensibility)
@router.api_route("POST", "/clean")
def clean_api(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle POST request"""
    try:
        body = json.loads(event.get("body", "{}"))
        endpoint = body.get("endpoint", "unknown")
        raw_data = body.get("data", [])
        if not raw_data:
            return {
                "statusCode": 400,
                "headers": router.default_headers,
                "body": json.dumps({"error": "Missing required field: data"}),
            }
        processed_data = data_cleaner.process_data(raw_data)
        if not processed_data:
            return {
                "statusCode": 200,
                "headers": router.default_headers,
                "body": json.dumps({"message": "No data to process", "endpoint": endpoint}),
            }
        return {
            "statusCode": 200,
            "headers": router.default_headers,
            "body": json.dumps(
                {
                    "message": "Data processed successfully",
                    "endpoint": endpoint,
                    "itemCount": len(processed_data),
                    "data": processed_data,
                },
                cls=DecimalEncoder,
            ),
        }

    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "headers": router.default_headers,
            "body": json.dumps({"error": "Invalid JSON in request body"}),
        }
    except ClientError as e:
        logger.error(f"AWS service error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": router.default_headers,
            "body": json.dumps({"error": f"Error interacting with AWS services: {str(e)}"}),
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": router.default_headers,
            "body": json.dumps({"error": f"Unexpected error occurred: {str(e)}"}),
        }


# Step Functions handler
@router.step_route("default")
def clean_step(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process Step Functions"""
    try:
        # Get parameters from event
        endpoint = event.get("endpoint")
        scrape_time = event.get("scrapeTime")
        data = event.get("data", [])

        # Validate required parameters
        missing_params = []
        if not endpoint:
            missing_params.append("endpoint")
        if not scrape_time:
            missing_params.append("scrapeTime")
        if not data:
            missing_params.append("data")
        if missing_params:
            return {"error": f"Missing parameters: {', '.join(missing_params)}"}

        # Clean data
        result = data_cleaner.process_data(data)
        return {
            "endpoint": endpoint,
            "scrapeTime": scrape_time,
            "data": result,
        }
    except Exception as e:
        logger.error(f"Error processing Step Functions task: {e}")
        return {"error": str(e)}


# Lambda handler function
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler function"""
    return router.handle(event, context)
