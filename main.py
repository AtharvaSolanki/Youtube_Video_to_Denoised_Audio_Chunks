from pytube import YouTube
import moviepy.editor as mp
import pandas as pd
from tinytag import TinyTag
import os
from pydub import AudioSegment
from pydub.silence import split_on_silence
from multiprocessing import Pool, cpu_count
from IPython import display as disp
from IPython.display import Audio
import torch
import torchaudio
from denoiser import pretrained
from denoiser.dsp import convert_audio
import matplotlib.pyplot as plt

links=pd.read_excel("Female_Marathi_Data.xlsx")
link=links["links"]

channel_name=[]
video_title=[]
video_link=[]
speaker_name=[]
audio_offset=[] #  number of bytes before audio data begins
bitdepth=[]  # bit depth for lossless audio
Bitrate=[]
duration=[]
file_size=[]
sample_rate=[]
duration_after_chunks=[]

a=0
for i in link:
  try:
    a=str(a)
    yt= YouTube(i)
    channel_name.append(yt.author)
    video_title.append(yt.title)
    video_link.append(i)
    filenam=yt.title
    youtubeObject = yt.streams.get_lowest_resolution()
    youtubeObject.download(filename=f"speaker.mp4")
    clip = mp.VideoFileClip(r"speaker.mp4")
    clip.audio.write_audiofile(r"speaker_"+a+".wav")
    audio = TinyTag.get("speaker_"+a+".wav")
    Bitrate.append(audio.bitrate)
    audio_offset.append(audio.audio_offset)
    bitdepth.append(audio.bitdepth)
    duration.append(audio.duration)
    file_size.append(str(audio.filesize/1000000))
    sample_rate.append(audio.samplerate)
    speaker_name.append("speaker_"+a+".wav")
    #reading from audio mp3 file
    sound = AudioSegment.from_wav("speaker_"+a+".wav")
    os.mkdir("/home/ubuntu/vlippr/youtube/marathi/female/speaker_"+a)
    audio_chunks = split_on_silence(sound, min_silence_len=400, silence_thresh=-35 )
    #loop is used to iterate over the output list
    n=0
    dura=0
    for i, chunk in enumerate(audio_chunks):
      try:
        n=str(n)
        output_file ="/home/ubuntu/vlippr/youtube/marathi/female/speaker_"+a+"/chunk{0}.wav".format(i)
        print("Exporting file", output_file)
        chunk.export(output_file, format="wav")
        audio = TinyTag.get(output_file)
        #duration.append(audio.duration)
        new=audio.duration
        dura=dura+new
        model = pretrained.dns64().cuda()
        wav, sr = torchaudio.load(output_file)
        wav = convert_audio(wav.cuda() ,sr, model.sample_rate, model.chin)
        with torch.no_grad():
            denoised = model(wav[None])[0]
        meta=disp.Audio(denoised.data.cpu().numpy(), rate=model.sample_rate)
        with open(output_file , "wb") as f:
          f.write(meta.data)
          n=int(n)
          n=n+1
        audio = TinyTag.get(output_file)
        new=audio.duration
        if new>20 or new<0.75:
          os.remove(output_file)
      except:
        print("Not found")
        pass




    duration_after_chunks.append(str(dura))
    os.remove("speaker_"+a+".wav")
    os.remove("speaker.mp4")
    a=int(a)
    a=a+1
  except:
    pass

df=pd.DataFrame(list(zip(channel_name ,video_title , speaker_name , video_link  , audio_offset, bitdepth , Bitrate , duration ,duration_after_chunks, file_size ,  sample_rate)),columns=["Channel Name","Video Title", "Speaker Name" , "Video Link" ,'Audio_Offset', 'Bitdepth' ,'Bitrate', 'Duration in Seconds' ,'duration after chunking' ,' File_Size in MB' , "Sample_Rate"])
df.to_csv("/home/ubuntu/vlippr/youtube/marathi/female_data.csv")