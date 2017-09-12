counter=0
while [ $? -eq 0 ]
do
echo 'Pass ==================' $counter
npm test
((++counter))
done
