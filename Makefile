create_docu:
	@echo "Creating documentation..."
	pandoc --highlight-style espresso -V colorlinks -V geometry:"top=2cm, bottom=2cm, left=2cm, right=2cm" --toc -s README.md -o dokumentation.pdf
