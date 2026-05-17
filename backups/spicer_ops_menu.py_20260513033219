#!/usr/bin/env python3

"""
spicer_ops_menu.py - Tools & Diagnostics Menu for the spicer directory

Features:
- Auto-discovers Python (.py) and shell (.sh) scripts
- Displays descriptions from docstrings or top comments
- Allows searching/filtering scripts
- User-friendly, visually clear CLI menu
- Runs scripts with correct interpreter
- Includes submenu for remote service ops
"""

import os
import subprocess
import sys
import re
import ast
from pathlib import Path
from textwrap import fill

# === Visual helpers ===
def color(text, code):
    return f"\033[{code}m{text}\033[0m"
def red(text): return color(text, '31')
def blue(text): return color(text, '34')
def cyan(text): return color(text, '36')
def green(text): return color(text, '32')
def yellow(text): return color(text, '33')
def magenta(text): return color(text, '35')
def bold(text): return f"\033[1m{text}\033[0m"
def underline(text): return f"\033[4m{text}\033[0m"
def gray(text): return color(text, '90')


CURRENT_DIR = Path(__file__).parent
NEXT_DIR = ""  # Placeholder for potential future use in navigating directories

DIRS = {
    'spicer': Path(__file__).parent,
    'scripts': Path(__file__).parent / 'scripts',
    'src': Path(__file__).parent / 'src',
    'data': Path(__file__).parent / 'data',
    'logs': Path(__file__).parent / 'logs'
}



# Splash menu with ASCII art and welcome message
def print_splash():
    ascii_art = r"""
   _____            
  / ===_|      @                        ////////  ////////  ////////  ////////////////////
 | (___   ___  _  ___   ____     ___    ///  ///  ///  ///    ///     //////  ///  /////// 
  \___ \ / _ \| |/ __\ / __ \|^^//^\\   ///  ///  ///  ///    ///     //////////////////// 
  ____) | |_| | | (___|  ^__/|  /       ////////  ////////    ///     ///  /////////  ////
 |_____/|  __/\__,___/ \____/|__|       ///  ///  ///         ///     //// //////// //////
        | |                             ///  ///  ///         ///     /////________///////
        |_|                             ///  ///  ///       ///////   //////////////////// 
    """
    print(bold(gray(ascii_art)))
    print(green(bold("Welcome to the Spicer API Handler Tools & Diagnostics Menu!")))
    print(gray("Select a script to run or search/filter the available tools.\n"))

# === Script discovery ===
# Parse list of directories to search for scripts, and partition them into categories based on their location (
# e.g., root spicer directory, scripts subdirectory, src subdirectory). This allows for better organization and potential 
# future features like category-based filtering or navigation. The get_scripts function will then only list .py and .sh files, 
# excluding hidden files, directories, and the menu script itself, while also sorting the scripts alphabetically for consistent ordering.
def arrangeScripts():
    # Define categories of usage for ops menu based on directory structure. 
    
    categories = {
        'Root Directory': DIRS['spicer'],
        'Diagnostic Scripts': DIRS['scripts'],
        'Core Modules': DIRS['src'],
        'Database Info': DIRS['data'],
        'Program Logs': DIRS['logs']
    }

    # Collect scripts from each category
    root_scripts = []
    script_scripts = []
    src_scripts = []
    data_scripts = []
    log_scripts = []

    for category, dir_path in categories.items():
        if dir_path.exists() and dir_path.is_dir():
            for item in dir_path.iterdir():
                if item.is_file() and item.suffix in ['.py', '.sh'] and not item.name.startswith('.') and item.name != Path(__file__).name:
                    if category == 'Root Directory':
                        root_scripts.append(item)
                    elif category == 'Diagnostic Scripts':
                        script_scripts.append(item)
                    elif category == 'Core Modules':
                        src_scripts.append(item)
                    elif category == 'Database Info':
                        data_scripts.append(item)
                    elif category == 'Program Logs':
                        log_scripts.append(item)
    # Sort scripts alphabetically within each category for consistent ordering in the menu
    root_scripts.sort()
    script_scripts.sort()
    src_scripts.sort()
    data_scripts.sort()
    log_scripts.sort()
    
    # Dictionary to hold categorized scripts for potential future use in category-based filtering or navigation
    categorized_scripts = {
        'Root Directory': root_scripts,
        'Diagnostic Scripts': script_scripts,
        'Core Modules': src_scripts,
        'Database Info': data_scripts,
        'Program Logs': log_scripts
    }
    return categorized_scripts
    

# === Directory navigation (optional) ===
# For simplicity, this example only allows navigating within the spicer directory and its subdirectories
def change_directory(path):

    # Prevent navigating outside of the spicer directory for security
    global CURRENT_DIR

    # Resolve the new path and ensure it's within the spicer directory
    new_path = (CURRENT_DIR / path).resolve()

    # Only allow changing to directories within the spicer directory
    if new_path.is_dir() and str(new_path).startswith(str(Path(__file__).parent.resolve())):
        CURRENT_DIR = new_path
    else:
        # Invalid directory - ignore and stay in current directory
        print(yellow('Invalid directory. Staying in current directory.'))

#=== Description extraction ===
# For Python scripts, it checks for a triple-quoted docstring at the top of the file. If found, it uses the first line of the docstring as the description.
# If no docstring is found, it looks for single-line comments at the top of the file (lines starting with #) and uses the first comment line as the description.
# For shell scripts, it looks for comment lines at the top of the file (ignoring the shebang line) and uses the first comment line as the description.
def get_description(path):

    # Read the first 5 lines of the file to look for docstrings or comments
    try:
        with open(path, 'r', encoding='utf-8') as f:
            lines = [next(f) for _ in range(5)]
    except Exception:
        return ''

    # Python docstring (full, not just first line)
    if path.suffix == '.py':
        try:
            with open(path, 'r', encoding='utf-8') as f:
                source = f.read(300)  # Read up to 300 chars for docstring
            module_doc = ast.get_docstring(ast.parse(source))
            if module_doc:
                return module_doc.strip()
        except Exception:
            pass
        # If no docstring, fall back to robust consecutive top comments
        comment_lines = []
        skip_lines = 0
        for idx, line in enumerate(lines):
            s = line.strip()
            # Skip shebang or encoding lines at the very top
            if idx == 0 and (s.startswith('#!') or s.startswith('# -*-')):
                skip_lines += 1
                continue
            if s.startswith('#'):
                comment_lines.append(s.lstrip('#').strip())
            elif s == '':
                continue  # allow blank lines before comments
            else:
                break  # stop at first non-comment, non-blank after block
        if comment_lines:
            return '\n'.join(comment_lines)

    # Shell script: skip shebang and blank lines, then collect first block of comments
    if path.suffix == '.sh':
        comment_lines = []
        found_comment = False
        started = False
        for idx, line in enumerate(lines):
            s = line.strip()
            if idx == 0 and s.startswith('#!'):
                continue
            if not started and s == '':
                continue  # skip blank lines after shebang
            if s.startswith('#'):
                comment_lines.append(s.lstrip('#').strip())
                found_comment = True
                started = True
            elif found_comment and s == '':
                comment_lines.append('')  # preserve blank lines in block
            elif found_comment:
                break  # stop at first non-comment, non-blank after block
            elif not found_comment and s != '':
                break  # stop if code appears before any comment
        if comment_lines:
            # Remove trailing blank lines
            while comment_lines and comment_lines[-1] == '':
                comment_lines.pop()
            return '\n'.join(comment_lines)
    return ''

#=== Menu display ===
# The menu displays the available scripts with their descriptions, and provides options for the user to select a script to run, search/filter scripts, change directories, or quit the menu.
def print_menu(categorized_scripts):
    # Clear the screen before displaying the menu for better readability
    os.system('clear' if os.name == 'posix' else 'cls')
    # Print the splash screen with ASCII art and welcome message at the top of the menu for a more engaging user experience. This will be displayed every time the menu is refreshed, providing a
    print_splash()
    # Display the current directory at the top of the menu so users always know where they are in the directory structure. This is especially helpful when navigating through subdirectories within the spicer directory.
    print(bold(underline('Spicer API Handler Tools & Diagnostics Menu')))
    print(gray(f'Current Directory: {CURRENT_DIR}/\n'))

    # Display scripts based on current directory and filter. Each script is listed with its name and a brief description extracted from the script's docstring or comments. The scripts are numbered for easy selection by the user.
    #Must be integers or slices, not str - so we check the current directory and display the appropriate category of scripts. If there is no filter text, we display all scripts in the current directory. If there is filter text, we create a new list of scripts that only includes those whose name or description contains the filter text (case-insensitive). This allows the user to quickly narrow down the list of scripts to find what they are looking for. The filtering checks both the script's filename and its description (extracted from docstrings or comments) to provide a more comprehensive search capability.
    if CURRENT_DIR == DIRS['spicer']:
        activeDir = categorized_scripts['Root Directory']
        category_name = 'Root Directory'
    elif CURRENT_DIR == DIRS['scripts']:
        activeDir = categorized_scripts['Diagnostic Scripts']
        category_name = 'Diagnostic Scripts'
    elif CURRENT_DIR == DIRS['src']:
        activeDir = categorized_scripts['Core Modules']
        category_name = 'Core Modules'
    elif CURRENT_DIR == DIRS['data']:
        activeDir = categorized_scripts['Database Info']
        category_name = 'Database Info'
    elif CURRENT_DIR == DIRS['logs']:
        activeDir = categorized_scripts['Program Logs']
        category_name = 'Program Logs'
    else:
        activeDir = []
        category_name = CURRENT_DIR.name

    if activeDir:
        print(bold(underline(f"{category_name}:")))
        for i, script in enumerate(activeDir, 1):
            desc = get_description(script)
            if not desc:
                desc = gray('(No description)')
            print(f"{cyan(str(i) + '.')} {blue(script.name)} - {desc}")
    else:
        print(red('No scripts found.'))
    print(bold(magenta("Type a number to run, 's' to search, 'r' for remote ops, 'q' to quit.")))
    print(bold(gray('To move directory enter: cd spicer/your/desired/path or cd spicer for root\n')))


#=== Script execution ===
# The run_script function takes the path of the selected script and executes it using the appropriate interpreter (Python for .py files and bash for .sh files). It also handles any errors that may occur during execution and prompts the user to return to the menu after the script finishes.
def run_script(script_path):

    # Display the script being run and execute it with the correct interpreter based on the file extension. If the script exits with an error, display a message and return to
    print(bold(f"\n--- Running: {script_path.name} ---\n"))
    if script_path.suffix == '.py':
        cmd = [sys.executable, str(script_path)]
    # For shell scripts, we use 'bash' to execute them. This assumes that the system has bash installed and available in the PATH. If the script has a shebang line (e.g., #!/bin/bash), it will still
    elif script_path.suffix == '.sh':
        cmd = ['bash', str(script_path)]
    # If the script has an unrecognized extension, we can print an error message and return to the menu without trying to execute it.
    else:
        print(yellow('Unknown script type.'))
        return
    # We use subprocess.run to execute the command and check=True to raise an exception if the script exits with a non-zero status. If an error occurs, we catch the CalledProcessError and print a message with the error code.
    try:
        subprocess.run(cmd, check=True)
    # If the script exits with a non-zero status, subprocess.run will raise a CalledProcessError, which we catch and print a message indicating that the script exited with an error code. After the script finishes (whether it succeeded or failed), we prompt the user to press Enter to return to the menu.
    except subprocess.CalledProcessError as e:
        print(yellow(f"Script exited with error code {e.returncode}"))
    input(gray('\nPress Enter to return to menu...'))



# === Main menu ===
# The main function initializes the menu by retrieving the list of scripts and entering a loop that displays the menu, handles user input for selecting scripts, searching/filtering, changing directories, and quitting the menu. It also refreshes the list of scripts after changing directories to ensure that the menu reflects the current directory's contents.
def main():

    # Initialize the menu by retrieving the list of scripts and entering a loop that displays the menu and handles user input for selecting scripts, searching/filtering, changing directories, and quitting the menu. It also refreshes the list of scripts after changing directories to ensure that the menu reflects the current directory's contents.
    categorized_scripts = arrangeScripts()

    # The main loop continues until the user chooses to quit by entering 'q'. Inside the loop, it filters the list of scripts based on the current filter text (if any), displays the menu, and prompts the user for input. Depending on the user's input, it handles directory changes, searching/filtering, script selection, and quitting the menu. After running a script or changing directories, it resets the filter text to ensure that the menu shows all scripts in the new context.
    while True:

        # Display the menu with the filtered list of scripts and prompt the user for input. The menu shows the available scripts with their descriptions, and provides options for the user to select a script to run, search/filter scripts, change directories, or quit the menu. The user can also enter 'cd' followed by a path to change directories within the spicer directory.
        print_menu(categorized_scripts)
        choice = input(bold('Select option: ')).strip()

        # Handle directory change
        if choice.startswith('cd '):
            path = choice[3:].strip()
            change_directory(path)
            categorized_scripts = arrangeScripts()  # Refresh scripts after changing directory
            filter_text = None  # Reset filter after changing directory
            continue
        # Handle quitting the menu
        if choice.lower() == 'q':
            print(green('Happy Coding!'))
            break
        # Handle searching/filtering scripts
        if choice.lower() == 's':
            filter_text = input('Enter search/filter text: ').strip()
            continue
        # Handle selecting a script to run
        if not choice.isdigit() or not (1 <= int(choice) <= len(categorized_scripts['Root Directory'] + categorized_scripts['Diagnostic Scripts'] + categorized_scripts['Core Modules'] + categorized_scripts['Database Info'] + categorized_scripts['Program Logs'])):
            print(red('Invalid selection.'))
            input(gray('Press Enter to continue...'))
            continue
        # If the user input is a valid number corresponding to one of the filtered scripts, we retrieve the selected script from the filtered list and call the run_script function to execute it. After running the script, we reset the filter text to ensure that the menu shows all scripts in the new context when it is displayed again.
        script = categorized_scripts['Root Directory'] + categorized_scripts['Diagnostic Scripts'] + categorized_scripts['Core Modules'] + categorized_scripts['Database Info'] + categorized_scripts['Program Logs'][int(choice)-1]
        run_script(script)
        filter_text = None  # Reset filter after running

# The if __name__ == '__main__': block ensures that the main function is only executed when the script is run directly, and not when it is imported as a module. This allows the script to be used as a standalone menu for running other scripts in the spicer directory, while also allowing its functions to be imported and used in other contexts if needed.
if __name__ == '__main__':
    main()

