#!/usr/bin/env python3
"""
Validation script for new MCP tools.
Tests that all new tools are properly registered and functional.
"""

import asyncio
import inspect
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import mcp_server.server as server_module


async def main():
    """Validate all new tools."""
    print("=" * 80)
    print("MCP Tools Enhancement Validation")
    print("=" * 80)

    # Get the list_tools function from the server module
    # Find the function with @app.list_tools() decorator
    list_tools_fn = None
    for name, obj in inspect.getmembers(server_module):
        if inspect.isfunction(obj) and name == "list_tools":
            list_tools_fn = obj
            break

    if list_tools_fn is None:
        print("ERROR: Could not find list_tools function")
        return 1

    tools = await list_tools_fn()
    tool_names = {tool.name for tool in tools}

    # New tools that should be registered
    new_tools = [
        "calculate_support_resistance_levels",
        "detect_candlestick_patterns",
        "detect_chart_patterns",
        "get_squeeze_indicators",
        "get_momentum_indicators",
        "get_volume_profile",
        "detect_volume_anomalies",
        "get_advanced_volume_indicators",
        "get_weekly_monthly_candles",
        "get_multi_timeframe_alignment",
        "calculate_relative_strength",
    ]

    print(f"\nTotal registered tools: {len(tools)}")
    print(f"New tools to validate: {len(new_tools)}\n")

    # Check each new tool
    validation_results = []
    for tool_name in new_tools:
        if tool_name in tool_names:
            # Get the tool definition
            tool = next(t for t in tools if t.name == tool_name)
            has_schema = tool.inputSchema is not None
            has_description = bool(tool.description)

            status = "✓ PASS" if (has_schema and has_description) else "✗ FAIL"
            validation_results.append((tool_name, status, has_schema, has_description))

            print(f"{status}: {tool_name}")
            if has_description:
                print(f"    Description: {tool.description[:80]}...")
            if has_schema:
                required = tool.inputSchema.get("required", [])
                properties = list(tool.inputSchema.get("properties", {}).keys())
                print(f"    Parameters: {', '.join(properties)}")
                print(f"    Required: {', '.join(required) if required else 'none'}")
            print()
        else:
            validation_results.append((tool_name, "✗ MISSING", False, False))
            print(f"✗ MISSING: {tool_name}\n")

    # Summary
    print("=" * 80)
    passed = sum(1 for _, status, _, _ in validation_results if status == "✓ PASS")
    failed = sum(1 for _, status, _, _ in validation_results if status != "✓ PASS")

    print(f"Validation Summary:")
    print(f"  Passed: {passed}/{len(new_tools)}")
    print(f"  Failed: {failed}/{len(new_tools)}")
    print("=" * 80)

    if failed == 0:
        print("\n✓ All new tools successfully registered and validated!")
        return 0
    else:
        print(f"\n✗ {failed} tool(s) failed validation")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
