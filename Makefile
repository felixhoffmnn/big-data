create_docu:
	@echo "Creating documentation..."
	pandoc README.md -o dokumentation.pdf --from markdown --template eisvogel --listings -V colorlinks -V geometry:"top=2cm, bottom=3cm, left=2cm, right=2cm" -s
