#!/bin/bash
counter=0
while [ $? -eq 0 ]
do
echo 'Pass ==================' $counter
((++counter))
npm test >test-pass-${counter}.log 2>&1
done
