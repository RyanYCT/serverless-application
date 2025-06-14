import decimal
import json
import logging
import os
from typing import Any, Dict

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


class DynamoDBService:
    """Service class for retrieving item IDs from DynamoDB"""

    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb")

    def get_item_id_list(self, table_name: str) -> Dict[str, Any]:
        """Get all item IDs from DynamoDB"""
        try:
            table = self.dynamodb.Table(table_name)
            projection = "id"

            # Scan the entire table
            scan_params = {"ProjectionExpression": projection}
            response = table.scan(**scan_params)
            items = response.get("Items", [])

            # Handle pagination
            while "LastEvaluatedKey" in response:
                scan_params["ExclusiveStartKey"] = response["LastEvaluatedKey"]
                response = table.scan(**scan_params)
                items.extend(response.get("Items", []))

            # Extract item IDs and convert decimal to int
            item_id_list = []
            for item in items:
                item_id_list.append(int(item["id"]))

            return {
                "message": "Items read successfully",
                "inputTable": table_name,
                "itemCount": len(item_id_list),
                "items": item_id_list,
            }

        except ClientError as e:
            logger.error(f"AWS Client Error: {e}")
            raise
        except KeyError as e:
            logger.error(f"Missing required parameter: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise


# Initialize router
router = LambdaRouter()

# Initialize service
id_service = DynamoDBService()


# API Gateway route handlers
@router.api_route("GET", "/idlist")
def retrieve_api(event: Dict[str, Any]) -> Dict[str, Any]:
    """Handle GET request"""
    try:
        # Parse the event
        query_params = event.get("queryStringParameters", {}) or {}

        # Get parameters from query parameters
        table_name = query_params.get("table_name", "") or os.getenv("DYNAMODB_TABLE_NAME", "")

        # Validate required parameters
        missing_params = []
        if not table_name:
            missing_params.append("table_name")
        if missing_params:
            return {
                "statusCode": 400,
                "headers": router.default_headers,
                "body": json.dumps({"error": f"Missing parameters: {', '.join(missing_params)}"}),
            }

        # Retrieve item id from DynamoDB table
        result = id_service.get_item_id_list(table_name)
        return {
            "statusCode": 200,
            "headers": router.default_headers,
            "body": json.dumps(result, cls=DecimalEncoder),
        }

    except Exception as e:
        logger.error(f"Unexpected errors: {e}")
        return {
            "statusCode": 500,
            "headers": router.default_headers,
            "body": json.dumps({"error": str(e)}),
        }


# Step Functions handler
@router.step_route("default")
def retrieve_step(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process Step Functions"""
    try:
        # Get parameters from Step Functions input event object
        table_name = event.get("table_name", "") or os.getenv("DYNAMODB_TABLE_NAME", "")
        endpoint = event.get("endpoint")

        # Validate required parameters
        missing_params = []
        if not table_name:
            missing_params.append("table_name")
        if not endpoint:
            missing_params.append("endpoint")
        if missing_params:
            return {"error": f"Missing parameters: {', '.join(missing_params)}"}

        # Retrieve item id from DynamoDB table
        result = id_service.get_item_id_list(table_name)
        return {
            "endpoint": endpoint,
            "tableName": table_name,
            "itemCount": result["itemCount"],
            "itemIDs": result["items"],
        }

    except Exception as e:
        logger.error(f"Unexpected errors: {e}")
        return {"error": str(e)}


# Lambda handler function
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler function"""
    return router.handle(event, context)
