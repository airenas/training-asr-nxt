#!/bin/bash
set -euo pipefail

wav_path=$1
rir_path=$2
rir_delay=$3
noise_path=$4
snr=$5
comp_alg=$6
comp_rate=$7
dst_f=$8
id=$9

log=$id.log
echo clean$id.raw > in$id.list
echo mixed$id.raw > out$id.list

# Convolution wit RIR step
# "none" means convolution step needs to be skipped
wav_in=$wav_path
if [ "$rir_path" != "none" ]; then
   echo "'"$rir_path"'" $rir_delay
   sox $wav_in -p pad $rir_delay 0 | sox - -b 16 tmp$id.wav fir $rir_path # rir txt
   sox tmp$id.wav tmpo$id.wav trim 0 -$rir_delay
   wav_in=tmpo$id.wav
   echo "Convolution with RIR $rir_path is done"
fi

# Add noise step
# "none" means noise addition step needs to be skipped
if [ "$noise_path" != "none" ]; then
   echo "Adding noise $noise_path with SNR $snr dB"
   sox "$wav_in" clean$id.raw
   fant/filter_add_noise -u -l -20 -i in$id.list -o out$id.list -n "$noise_path" -s $snr -e $id.log > /dev/null
   sox -r 16000 -e signed -b 16 -c 1 mixed$id.raw tmpo$id.wav
   wav_in=tmpo$id.wav
fi

# Add compression/decompression step
if [ "$comp_alg" != "none" ]; then
    :
      echo "Applying compression/decompression with $comp_alg at $comp_rate bps"
    #echo $wav_path
    #ffmpeg -y -nostats -i "$wav_in" -ac 1 -ab $comp_rate $comp_rate.$id.$comp_alg
    #ffmpeg -y -nostats -i $comp_rate.$id.$comp_alg -ac 1 -ar 16000 tmpo$id.wav
    # Compute duration of the initial file
    nb_org_samples=$(soxi -s "$wav_in")
    #echo $nb_org_samples
    lame --quiet "$wav_in" -b $comp_rate $comp_rate.$id.$comp_alg
    lame --quiet --decode $comp_rate.$id.$comp_alg tmpo$id.wav
    # Compute duration of the modified file
    nb_mod_samples=$(soxi -s tmpo$id.wav)
    #echo $nb_mod_samples
    # Modified file sometimes becomes longer after compression/decompression
    # so cut 1/2 and 1/2 of added duration from start and end respectively
    if (( nb_mod_samples > nb_org_samples )); then
       d=$((nb_mod_samples - nb_org_samples))
       d1=$(( d / 2))
       d2=$(( d - d1 ))
       echo "$wav_path is $d samples longer after $comp_alg $comp_rate format manipulation"
       cp tmpo$id.wav tmp$id.wav
       sox tmp$id.wav tmpo$id.wav trim ${d1}s -${d2}s # s means samples not seconds
    fi
    rm $comp_rate.$id.$comp_alg
    wav_in=tmpo$id.wav
fi

mv "$wav_in" "$dst_f"
rm -f tmp$id.wav tmpo$id.wav clean$id.raw mixed$id.raw in$id.list out$id.list
exit 0
