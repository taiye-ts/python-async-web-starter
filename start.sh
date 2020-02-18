#/bin/bash

echo Enter project name
read project_name

function sed_files() {
  local arr=("$@")
  echo "${arr[@]}"
  for entry in "${arr[@]}"
    do
      sed -i '' "s/project_name/$project_name/g" $entry
    done
}

files_to_sed=($(find -E . -type f -regex "^.*.py"))
sed_files "${files_to_sed[@]}"

additional_sed_files=(
  README_after_generate.md
  Makefile
)
sed_files "${additional_sed_files[@]}"

mv src/project_name src/$project_name
mv src/tests/project_name src/tests/$project_name
rm README.md
mv README_after_generate README.md

echo Done
