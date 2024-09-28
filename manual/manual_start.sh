#!/bin/bash

# Echo to indicate start of the program
echo "STARTING PROGRAM..."

# Define the path to the virtual environment (can be modified if needed)
VENV_PATH="../venv"

# Check if the virtual environment exists
if [ -d "$VENV_PATH" ]; then
    # Activate the virtual environment
    source "$VENV_PATH/bin/activate"
else
    echo "Warning: Virtual environment not found at $VENV_PATH. Proceeding without activation."
fi

PYTHON_FILE=""

# Check if a file name is provided as an argument
if [ $# -eq 0 ]; then
    # If no argument is provided, ask the user for a filename
    read -p "Please enter the Python file name: " PYTHON_FILE
else
    # If an argument is provided, use it as the filename
    PYTHON_FILE="$1"
fi

# Check if the file exists
if [ ! -f "$PYTHON_FILE" ]; then
    echo "Error: File '$PYTHON_FILE' not found."
    exit 1
fi

# Echo to indicate the start of the Python script
echo "*** BEGIN PROGRAM ***"

# Run the Python script with the provided file name
python "$PYTHON_FILE"

# Echo to indicate the end of the Python script
echo "*** END PROGRAM ***"

# Deactivate the virtual environment
deactivate

# Echo to indicate program completion
echo "PROGRAM EXECUTION COMPLETE."
