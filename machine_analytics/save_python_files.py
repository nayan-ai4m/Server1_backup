import os
import shutil

# Set the directory to save collected Python files
output_dir = "python_files"

# Create the directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Walk through the current directory and its subdirectories
for root, dirs, files in os.walk("."):
    for file in files:
        if file.endswith(".py"):
            full_path = os.path.join(root, file)
            # Avoid copying files already in the output_dir
            if os.path.abspath(output_dir) not in os.path.abspath(full_path):
                dest_path = os.path.join(output_dir, file)
                
                # Handle filename collisions
                base, ext = os.path.splitext(file)
                counter = 1
                while os.path.exists(dest_path):
                    dest_path = os.path.join(output_dir, f"{base}_{counter}{ext}")
                    counter += 1

                shutil.copy2(full_path, dest_path)

print(f"All Python files have been collected in the '{output_dir}' folder.")
