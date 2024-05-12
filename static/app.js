let audio = null;
let currentTrackIndex = 0;
const tracks = audioFiles; // Use the audioFiles object instead of {{ tracks | tojson }}

document.getElementById('play-pause').addEventListener('click', togglePlayPause);
document.getElementById('prev').addEventListener('click', playPrevious);
document.getElementById('next').addEventListener('click', playNext);

function playSong(url, title, artist) {
    if (audio) {
        audio.pause();
    }
    audio = new Audio(url);
    audio.play();
    updateNowPlaying(title, artist);
    document.getElementById('play-pause').textContent = 'Pause';
}

function togglePlayPause() {
    if (audio) {
        if (audio.paused) {
            audio.play();
            document.getElementById('play-pause').textContent = 'Pause';
        } else {
            audio.pause();
            document.getElementById('play-pause').textContent = 'Play';
        }
    }
}

function playNext() {
    currentTrackIndex = (currentTrackIndex + 1) % tracks.length;
    const track = tracks[currentTrackIndex];
    playSong(track.url, track.title, track.artist); // Use track.url instead of '{{ url_for("static", filename="audio/") }}' + track.file_name
}

function playPrevious() {
    currentTrackIndex = (currentTrackIndex - 1 + tracks.length) % tracks.length;
    const track = tracks[currentTrackIndex];
    playSong(track.url, track.title, track.artist); // Use track.url instead of '{{ url_for("static", filename="audio/") }}' + track.file_name
}

function updateNowPlaying(title, artist) {
    document.getElementById('now-playing-text').textContent = `${title} by ${artist}`;
}