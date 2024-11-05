#!/bin/bash

# Script to update a submodule in the scrape_the_law project

# Ensure we're in the correct directory
cd "$(dirname "$0")"

# Function to update a submodule
update_submodule() {
    local submodule=$1
    echo "Updating $submodule submodule..."
    
    # Navigate to the submodule directory
    cd $submodule

    # Ensure we're on the main branch
    git checkout main

    # Pull the latest changes
    git pull origin main

    # Make your changes here
    # For example: vim some_file.py

    # Stage all changes
    git add .

    # Commit the changes
    read -p "Enter commit message for $submodule: " commit_message
    git commit -m "$commit_message"

    # Push changes to the submodule's remote repository
    git push origin main

    # Return to the main project directory
    cd ..

    # Stage the submodule update in the main project
    git add $submodule

    # Commit the submodule update
    git commit -m "Update $submodule submodule"
}

# Main script execution
echo "Which submodule do you want to update? (database/logger/utils)"
read submodule_choice

case $submodule_choice in
    database|logger|utils)
        update_submodule $submodule_choice
        ;;
    *)
        echo "Invalid choice. Please choose database, logger, or utils."
        exit 1
        ;;
esac

# Push changes in the main project
git push origin main

echo "Submodule update complete and pushed to main repository."