import os
import torchaudio
from speechbrain.inference.classifiers import EncoderClassifier

# Load the pretrained language ID model
language_id = EncoderClassifier.from_hparams(
    source="speechbrain/lang-id-voxlingua107-ecapa",
    savedir="tmp/lang-id"
)

EXPECTED_LANG = "lt"
CONFIDENCE_THRESHOLD = 0.80  

AUDIO_DIR = "/home/airenas/projects/training-asr-nxt/tests"

# Helper to analyze one file
def check_if_lt(audio_path):
    try:
        signal = language_id.load_audio(audio_path)
        prediction = language_id.classify_batch(signal)
        _, log_prob_target, _, predicted_lang = prediction

        predicted = predicted_lang[0]
        confidence = log_prob_target.exp().item()

        if predicted.startswith(EXPECTED_LANG) and confidence > CONFIDENCE_THRESHOLD:
            status = "✅ LT"
        else:
            status = "❌ Not LT or low confidence"

        print(f"{os.path.basename(audio_path):30} -> {predicted:6} | Confidence: {confidence:.2f} | {status}")

    except Exception as e:
        print(f"{audio_path}: ERROR - {e}")

# Process all audio files in the directory
for file in os.listdir(AUDIO_DIR):
    if file.endswith(".m4a"):  # or change to .m4a if using ffmpeg support
        check_if_lt(os.path.join(AUDIO_DIR, file))
