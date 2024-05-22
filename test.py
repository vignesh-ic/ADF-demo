import sys, json

# Check if arguments are provided
if len(sys.argv) < 2:
    print("Usage: python script.py <argument>")
    sys.exit(1)

# Access the arguments
argument = sys.argv[1]
config=""
with open(str(argument), 'r') as file:
        config = json.load(file)
print("Argument passed:", config)