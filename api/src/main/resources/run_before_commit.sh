echo Some useful stuff to run before comitting to git
echo
echo running unit tests:
nosetests
echo
echo
echo Lint summary
find . -name "*.py" | xargs /usr/local/bin/pylint | grep  -C 5 "[C\|W\|R\|E]:\|has been rated"
echo done

