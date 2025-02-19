cat ~/Save/rce.pw || exit # show the password

cd .. || exit
rsync -avz map-reduce varun.edachali@rce.iiit.ac.in:~
cd - || exit
