---
type: "agent_requested"
description: "Guidelines on how to write tests"
---
# Test Guidelines

## Framework and Structure
- Use **pytest** to write tests
- Only use **test methods**, not test classes
- Every test module starts with `test_`
- Every test method starts with `test_`
- Organize test files to mirror the source code structure

## Test Design Principles
- Document all tests with clear docstrings
- Follow the pattern: **setup → execution → assertion**
- Keep tests focused - one test should verify one specific behavior
- Use descriptive test method names that explain what is being tested
- If meaningful, put more than one test case into one test method

## Fixtures and Dependencies
- Make use of existing fixtures
- Create fixtures if meaningful for reusability
- Prefer using actual objects over mocks when the dependency is available
- Clean up resources in tests (use `tmp_path` fixture for temporary files)

## Testing Techniques
- Use `@pytest.mark.parametrize` decorator for testing multiple similar scenarios
- Use meaningful assertion messages when helpful for debugging
- Avoid testing implementation details - focus on behavior and outcomes

## Examples
```python
def test_calculate_daily_start_quarter_regular_transitions():
    """Test calculating the next quarter for Q1, Q2, and Q3 transitions."""
    # Setup
    test_cases = [
        ("2022q1", 2022, 2),
        ("2022q2", 2022, 3),
        ("2022q3", 2022, 4)
    ]

    # Execution & Assertion
    for input_quarter, expected_year, expected_qrtr in test_cases:
        result = DailyPreparationProcess._calculate_daily_start_quarter(input_quarter)
        assert result.year == expected_year
        assert result.qrtr == expected_qrtr
```

